import asyncio
import logging
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from integrations.seasonarr.db.models import SonarrInstance, ActivityLog
from integrations.seasonarr.clients.sonarr_client import SonarrClient
from integrations.seasonarr.core.websocket_manager import manager
from integrations.seasonarr.services.bulk_operation_manager import bulk_operation_manager
from datetime import datetime
from program.settings.manager import config_manager
from integrations.seasonarr.clients.alldebrid_cache_client import AllDebridCacheClient

logger = logging.getLogger(__name__)

class SeasonItService:
    def __init__(self, db: Session, user_id: int):
        self.db = db
        self.user_id = user_id

    def _create_activity_log(self, instance_id: int, show_id: int, show_title: str, season_number: Optional[int] = None) -> ActivityLog:
        """Crée une nouvelle entrée dans le journal d'activité"""
        activity = ActivityLog(
            user_id=self.user_id,
            instance_id=instance_id,
            action_type="season_it",
            show_id=show_id,
            show_title=show_title,
            season_number=season_number,
            status="in_progress",
            message=f"Démarrage de Season It pour {show_title}" + (f" saison {season_number}" if season_number else " toutes saisons")
        )
        self.db.add(activity)
        self.db.commit()
        self.db.refresh(activity)
        return activity

    def _update_activity_log(self, activity: ActivityLog, status: str, message: str = None, error_details: str = None):
        """Met à jour une entrée du journal d'activité"""
        activity.status = status
        if message:
            activity.message = message
        if error_details:
            activity.error_details = error_details
        if status in ["success", "error"]:
            activity.completed_at = datetime.utcnow()
        self.db.commit()

    async def process_season_it(self, show_id: int, season_number: Optional[int] = None, instance_id: Optional[int] = None) -> Dict[str, Any]:
        activity = None
        try:
            # Get series data first so we can include poster info
            instance = self._get_sonarr_instance_by_id(instance_id) if instance_id else self._get_sonarr_instance(show_id)
            if not instance:
                raise Exception("Aucune instance Sonarr trouvée pour cette série")

            client = SonarrClient(instance.url, instance.api_key, instance.id)
            series_data = await self._get_series_data(client, show_id)
            show_title = series_data.get("title", "Unknown Show")
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)

            # Send enhanced progress update instead of regular one
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_single" if season_number else "season_it_all",
                "🚀 Initialisation du traitement Season It...", 
                10,
                current_step="Initialize",
                details={"poster_url": poster_url, "season_number": season_number}
            )

            # Create activity log entry
            activity = self._create_activity_log(instance.id, show_id, show_title, season_number)

            if season_number:
                result = await self._process_single_season_with_data(client, show_id, season_number, show_title, series_data)
            else:
                result = await self._process_all_seasons(client, show_id, show_title, series_data)

            # Update activity log on success
            self._update_activity_log(
                activity, 
                "success", 
                f"Season It terminé avec succès pour {show_title}" + (f" saison {season_number}" if season_number else " toutes saisons")            )
            
            return result

        except Exception as e:
            # Update activity log on error
            if activity:
                self._update_activity_log(
                    activity, 
                    "error", 
                    f"Season It a échoué pour {activity.show_title}",
                    str(e)
                )
            
            await manager.send_enhanced_progress_update(
                self.user_id, 
                activity.show_title if activity else "Unknown Show",
                "season_it_error",
                f"❌ Échec de Season It : {str(e)}", 
                100, 
                "error",
                current_step="Error",
                details={"error": str(e)}
            )
            raise

    def _get_primary_alldebrid_api_key(self) -> Optional[str]:
        """
        Récupère une clé AllDebrid depuis les instances SSD.

        Priorité :
        - config.alldebrid_instances activées
        - tri par priority
        - première instance valide

        L'ancien champ config.alldebrid_api_key est volontairement ignoré.
        """
        instances = getattr(config_manager.config, "alldebrid_instances", []) or []

        enabled_instances = []

        for inst in instances:
            if isinstance(inst, dict):
                enabled = inst.get("enabled", True)
                api_key = inst.get("api_key")
            else:
                enabled = getattr(inst, "enabled", True)
                api_key = getattr(inst, "api_key", None)

            if enabled and api_key:
                enabled_instances.append(inst)

        if not enabled_instances:
            return None

        def get_priority(inst):
            if isinstance(inst, dict):
                return inst.get("priority", 9999)

            return getattr(inst, "priority", 9999)

        enabled_instances.sort(key=get_priority)

        first = enabled_instances[0]

        if isinstance(first, dict):
            return str(first.get("api_key"))

        return str(getattr(first, "api_key"))

    def _is_sonarr_valid_release(self, release: Dict[str, Any]) -> bool:
        """
        Sonarr doit rester le premier filtre.
        On garde uniquement les releases que Sonarr n'a pas rejetées
        et qu'on peut télécharger précisément via guid + indexerId.
        """
        if release.get("approved") is False:
            return False

        if not release.get("guid"):
            return False

        indexer_id = release.get("indexerId")
        if not indexer_id:
            return False

        try:
            if int(indexer_id) <= 0:
                return False
        except Exception:
            return False

        return True

    def _is_existing_file_only_rejected_release(self, release: Dict[str, Any]) -> bool:
        """
        Autorise uniquement les releases rejetées par Sonarr parce qu'un fichier existe déjà
        avec un score/préférence égal ou supérieur.

        On refuse toujours :
        - mauvaise série
        - mauvaise saison
        - blocklist
        - pas assez de seeders
        - termes requis manquants
        - épisode non demandé
        - tout autre rejet Sonarr
        """
        if release.get("approved") is True:
            return False

        if not release.get("rejected"):
            return False

        if not release.get("fullSeason"):
            return False

        if not release.get("guid"):
            return False

        indexer_id = release.get("indexerId")
        if not indexer_id:
            return False

        try:
            if int(indexer_id) <= 0:
                return False
        except Exception:
            return False

        rejections = release.get("rejections") or []

        if not rejections:
            return False

        allowed_rejections = (
            "Existing file on disk has a equal or higher Custom Format score",
            "Existing file on disk is of equal or higher preference",
        )

        for reason in rejections:
            reason_text = str(reason)

            if not any(allowed in reason_text for allowed in allowed_rejections):
                return False

        return True

    def _release_cache_identity(self, release: Dict[str, Any]) -> Optional[str]:
        """
        Valeur utilisée pour vérifier le cache AD.
        Idéalement un magnet ou un infoHash.

        Attention :
        guid/downloadUrl ne sont pas toujours des hashes.
        Si aucun hash/magnet exploitable n'existe, on ne confirme pas le cache.
        """
        for key in (
            "magnetUrl",
            "magnet_url",
            "infoHash",
            "info_hash",
            "hash",
        ):
            value = release.get(key)
            if value:
                return str(value)

        return None

    async def _find_sonarr_valid_cached_release(
        self,
        releases: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        Retourne une release qui est :
        1. validée par Sonarr,
        ou rejetée uniquement parce que des fichiers existent déjà,
        2. téléchargeable précisément via guid + indexerId,
        3. confirmée cache chez AllDebrid.

        Si doute => None.
        """
        api_key = self._get_primary_alldebrid_api_key()

        if not api_key:
            logger.warning(
                "SeasonIt - Arrêt sécurisé : aucune clé AllDebrid active trouvée"
            )
            return None

        approved_releases = [
            release
            for release in releases
            if self._is_sonarr_valid_release(release)
        ]

        existing_file_only_rejected_releases = [
            release
            for release in releases
            if self._is_existing_file_only_rejected_release(release)
        ]

        approved_releases.sort(
            key=lambda release: (
                int(release.get("customFormatScore") or 0),
                int(release.get("seeders") or 0),
            ),
            reverse=True,
        )

        existing_file_only_rejected_releases.sort(
            key=lambda release: (
                int(release.get("customFormatScore") or 0),
                int(release.get("seeders") or 0),
            ),
            reverse=True,
        )

        valid_releases = approved_releases + existing_file_only_rejected_releases

        logger.info(
            "SeasonIt - Packs éligibles : %s validé(s) par Sonarr, "
            "%s refusé(s) uniquement à cause des fichiers déjà présents, "
            "%s testé(s) au total sur %s",
            len(approved_releases),
            len(existing_file_only_rejected_releases),
            len(valid_releases),
            len(releases),
        )

        if not valid_releases:
            logger.warning(
                "SeasonIt - Arrêt sécurisé : aucun pack éligible trouvé parmi %s release(s)",
                len(releases),
            )
            return None

        cache_values = []

        for release in valid_releases:
            cache_identity = self._release_cache_identity(release)

            if cache_identity:
                cache_values.append(
                    {
                        "hash": cache_identity,
                        "name": (
                            release.get("title")
                            or release.get("releaseTitle")
                            or release.get("guid")
                            or cache_identity
                        ),
                    }
                )
            else:
                logger.debug(
                    "SeasonIt - Pack ignoré pour le cache AllDebrid car aucun hash/magnet exploitable : %s",
                    release.get("title"),
                )

        logger.info(
            "SeasonIt - Vérification cache AllDebrid : %s pack(s) avec hash/magnet sur %s pack(s) éligible(s)",
            len(cache_values),
            len(valid_releases),
        )

        if not cache_values:
            logger.warning(
                "SeasonIt - Arrêt sécurisé : aucun pack éligible ne contient de hash/magnet exploitable"
            )
            return None

        ad_client = AllDebridCacheClient(api_key)
        cached_hashes = await ad_client.get_cached_hashes(cache_values)

        logger.info(
            "SeasonIt - Cache AllDebrid : %s pack(s) confirmé(s) sur %s testé(s)",
            len(cached_hashes),
            len(cache_values),
        )

        if not cached_hashes:
            logger.warning(
                "SeasonIt - Arrêt sécurisé : aucun pack éligible confirmé en cache AllDebrid"
            )
            return None

        for release in valid_releases:
            cache_identity = self._release_cache_identity(release)
            info_hash = ad_client.extract_info_hash(cache_identity)

            if info_hash and info_hash.lower() in cached_hashes:
                mode = (
                    "validé par Sonarr"
                    if self._is_sonarr_valid_release(release)
                    else "accepté car refus uniquement dû aux fichiers déjà présents"
                )

                logger.info(
                    "SeasonIt - Pack retenu : %s | mode=%s | score=%s | seeders=%s",
                    release.get("title"),
                    mode,
                    release.get("customFormatScore"),
                    release.get("seeders"),
                )

                return release

        logger.warning(
            "SeasonIt - Arrêt sécurisé : cache AllDebrid confirmé mais aucune correspondance fiable avec les packs éligibles"
        )
        return None

    async def _process_single_season_with_data(self, client: SonarrClient, show_id: int, season_number: int, show_title: str, series_data: Dict) -> Dict[str, Any]:
        """Traitement détaillé d'une saison avec suivi de progression"""
        
        # Récupère l'URL du poster depuis les données de la série
        poster_url = client._get_poster_url(series_data.get("images", []), client.instance_id)
        
        return await self._process_single_season(client, show_id, season_number, show_title, poster_url, series_data)

    async def _process_single_season(self, client: SonarrClient, show_id: int, season_number: int, show_title: str, poster_url: str = None, series_data: Dict = None) -> Dict[str, Any]:
        """Traitement sécurisé d'une saison avec validation Sonarr + cache AllDebrid"""

        # Si series_data n'est pas fourni, on le récupère
        if series_data is None:
            series_data = await self._get_series_data(client, show_id)
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)

        # Step 1: Initialize process
        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            f"🔄 Initialisation de Season It pour {show_title} saison {season_number}...",
            5,
            current_step="Initialisation",
            details={"poster_url": poster_url, "season_number": season_number},
        )

        # Step 2: Check for future episodes
        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            f"📅 Vérification des épisodes non diffusés pour la saison {season_number}...",
            8,
            current_step="Vérification épisodes futurs",
            details={"poster_url": poster_url, "season_number": season_number},
        )

        future_check = await client.has_future_episodes(show_id, season_number)

        if season_number in future_check.get("seasons_incomplete", []):
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "season_it_single",
                f"⏳ La saison {season_number} de '{show_title}' contient des épisodes non diffusés. Season It est ignoré pour éviter les packs incomplets.",
                100,
                "warning",
                current_step="Terminé",
                details={"poster_url": poster_url, "season_number": season_number},
            )

            return {
                "status": "incomplete_season",
                "message": "La saison contient des épisodes non diffusés",
            }

        # Step 3: Validate series data
        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            f"📋 Validation des données de la série {show_title}...",
            10,
            current_step="Validation des données",
            details={"poster_url": poster_url, "season_number": season_number},
        )

        # Step 4: Check for missing episodes
        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            f"🔍 Recherche des épisodes manquants dans la saison {season_number}...",
            15,
            current_step="Analyse des épisodes",
            details={"poster_url": poster_url, "season_number": season_number},
        )

        missing_data = await client.get_missing_episodes(show_id, season_number)
        seasons_with_missing = missing_data.get("seasons_with_missing", {})
        missing_episodes = seasons_with_missing.get(season_number, [])

        missing_list = [
            f"S{ep['seasonNumber']:02d}E{ep['episodeNumber']:02d} - {ep['title']}"
            for ep in missing_episodes
        ]

        logger.info(
            "SeasonIt - %s S%s : %s épisode(s) manquant(s)",
            show_title,
            season_number,
            len(missing_list),
        )

        if season_number not in seasons_with_missing:
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "season_it_single",
                f"✅ La saison {season_number} de '{show_title}' n'a aucun épisode manquant",
                100,
                "warning",
                current_step="Terminé",
                details={"poster_url": poster_url, "season_number": season_number},
            )

            return {
                "status": "no_missing_episodes",
                "message": "Aucun épisode manquant trouvé",
            }

        missing_count = len(seasons_with_missing[season_number])

        # Step 5: Load settings
        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            "⚙️ Chargement des préférences utilisateur et des paramètres...",
            20,
            current_step="Chargement des paramètres",
            details={
                "poster_url": poster_url,
                "season_number": season_number,
                "missing_count": missing_count,
            },
        )

        ssd_config = config_manager.config

        disable_season_pack_check = bool(
            getattr(ssd_config, "disable_season_pack_check", False)
        )

        skip_episode_deletion = bool(
            getattr(ssd_config, "skip_episode_deletion", False)
        )

        require_cached_pack_before_deletion = bool(
            getattr(ssd_config, "require_cached_pack_before_deletion", False)
        )

        logger.info(
            "SeasonIt - Mode sécurisé pour %s S%s : suppression=%s, vérification cache avant suppression=%s, recherche pack=%s",
            show_title,
            season_number,
            not skip_episode_deletion,
            require_cached_pack_before_deletion,
            not disable_season_pack_check,
        )

        # Step 6: Determine strategy
        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            f"🎯 Détermination de la stratégie optimale pour {missing_count} épisode(s) manquant(s)...",
            25,
            current_step="Stratégie",
            details={
                "poster_url": poster_url,
                "season_number": season_number,
                "missing_count": missing_count,
            },
        )

        # ------------------------------------------------------------------
        # CASE A: Season pack check disabled
        # ------------------------------------------------------------------
        if disable_season_pack_check:
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "season_it_single",
                "📝 Recherche de packs saison désactivée : utilisation de la recherche saison classique...",
                30,
                current_step="Recherche pack désactivée",
                details={
                    "poster_url": poster_url,
                    "season_number": season_number,
                    "missing_count": missing_count,
                },
            )

            if skip_episode_deletion:
                await manager.send_enhanced_progress_update(
                    self.user_id,
                    show_title,
                    "season_it_single",
                    "⚠️ Suppression des épisodes ignorée selon les paramètres utilisateur...",
                    35,
                    current_step="Suppression ignorée",
                    details={
                        "poster_url": poster_url,
                        "season_number": season_number,
                        "missing_count": missing_count,
                    },
                )

                logger.info(
                    f"Pas de suppression des épisodes pour {show_title} S{season_number}"
                )

            else:
                if require_cached_pack_before_deletion:
                    logger.warning(
                        "Suppression annulée : require_cached_pack_before_deletion=True "
                        "mais la recherche de packs saison est désactivée"
                    )

                    await manager.send_enhanced_progress_update(
                        self.user_id,
                        show_title,
                        "season_it_single",
                        "🛡️ Suppression annulée : cache pack requis mais recherche pack désactivée",
                        100,
                        "warning",
                        current_step="Arrêt sécurisé",
                        details={
                            "poster_url": poster_url,
                            "season_number": season_number,
                            "missing_count": missing_count,
                        },
                    )

                    return {
                        "status": "safe_stop_pack_check_disabled",
                        "season": season_number,
                        "show": show_title,
                        "missing_episodes": missing_count,
                        "message": (
                            "Suppression annulée : la vérification cache pack est requise, "
                            "mais la recherche de packs saison est désactivée."
                        ),
                    }

                await manager.send_enhanced_progress_update(
                    self.user_id,
                    show_title,
                    "season_it_single",
                    f"🗑️ Préparation de la suppression de {missing_count} épisode(s)...",
                    35,
                    current_step="Préparation suppression",
                    details={
                        "poster_url": poster_url,
                        "season_number": season_number,
                        "missing_count": missing_count,
                    },
                )

                await manager.send_enhanced_progress_update(
                    self.user_id,
                    show_title,
                    "season_it_single",
                    f"🧹 Suppression des épisodes existants de la saison {season_number}...",
                    40,
                    current_step="Suppression",
                    details={
                        "poster_url": poster_url,
                        "season_number": season_number,
                        "missing_count": missing_count,
                    },
                )

                logger.info(
                    f"Suppression des épisodes pour la série {show_title} saison {season_number}"
                )

                await client.delete_season_episodes(show_id, season_number)

                await manager.send_enhanced_progress_update(
                    self.user_id,
                    show_title,
                    "season_it_single",
                    "✅ Suppression des épisodes terminée avec succès...",
                    45,
                    current_step="Confirmation suppression",
                    details={
                        "poster_url": poster_url,
                        "season_number": season_number,
                        "missing_count": missing_count,
                    },
                )

        # ------------------------------------------------------------------
        # CASE B: Season pack check enabled
        # ------------------------------------------------------------------
        else:
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "season_it_single",
                "🔍 Searching for available season packs...",
                30,
                current_step="Search Season Packs",
                details={
                    "poster_url": poster_url,
                    "season_number": season_number,
                    "missing_count": missing_count,
                },
            )

            logger.info(
                f"Recherche de packs saison pour {show_title} S{season_number}"
            )

            releases = await client._get_releases(
                show_id,
                season_number,
                show_title,
            )

            logger.info(
                "SeasonIt - %s pack(s) saison trouvé(s) pour %s S%s",
                len(releases),
                show_title,
                season_number,
            )

            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "season_it_single",
                f"📊 Analyse de {len(releases)} release(s) saison disponible(s)...",
                35,
                current_step="Analyse des packs saison",
                current_step_number=7,
                total_steps=17,
                details={
                    "poster_url": poster_url,
                    "season_number": season_number,
                    "missing_count": missing_count,
                    "releases_found": len(releases),
                },
            )

            if not releases:
                await manager.send_enhanced_progress_update(
                    self.user_id,
                    show_title,
                    "season_it_single",
                    "❌ Aucun pack saison trouvé : retour à une recherche saison classique...",
                    40,
                    "warning",
                    current_step="Aucun pack saison",
                    details={
                        "poster_url": poster_url,
                        "season_number": season_number,
                        "missing_count": missing_count,
                    },
                )

            else:
                await manager.send_enhanced_progress_update(
                    self.user_id,
                    show_title,
                    "season_it_single",
                    "🎯 Releases saison trouvées : préparation de la stratégie...",
                    40,
                    current_step="Packs saison trouvés",
                    details={
                        "poster_url": poster_url,
                        "season_number": season_number,
                        "missing_count": missing_count,
                        "releases_found": len(releases),
                    },
                )

                if skip_episode_deletion:
                    await manager.send_enhanced_progress_update(
                        self.user_id,
                        show_title,
                        "season_it_single",
                        "⚠️ Suppression des épisodes ignorée selon les paramètres utilisateur...",
                        45,
                        current_step="Suppression ignorée",
                        details={
                            "poster_url": poster_url,
                            "season_number": season_number,
                            "missing_count": missing_count,
                        },
                    )

                    logger.info(
                        f"Suppression des épisodes ignorée pour {show_title} S{season_number} selon les paramètres utilisateur"
                    )

                else:
                    selected_release = None

                    if require_cached_pack_before_deletion:
                        await manager.send_enhanced_progress_update(
                            self.user_id,
                            show_title,
                            "season_it_single",
                            "🧪 Vérification Sonarr + cache AllDebrid avant suppression...",
                            45,
                            current_step="Vérification Sonarr + cache AD",
                            details={
                                "poster_url": poster_url,
                                "season_number": season_number,
                                "missing_count": missing_count,
                            },
                        )

                        selected_release = await self._find_sonarr_valid_cached_release(
                            releases
                        )

                        if not selected_release:
                            logger.warning(
                                "Suppression annulée : aucune release validée par Sonarr "
                                "et confirmée cache AllDebrid pour %s S%s",
                                show_title,
                                season_number,
                            )

                            await manager.send_enhanced_progress_update(
                                self.user_id,
                                show_title,
                                "season_it_single",
                                "🛡️ Aucun pack validé/cache confirmé : suppression annulée",
                                100,
                                "warning",
                                current_step="Arrêt sécurisé",
                                details={
                                    "poster_url": poster_url,
                                    "season_number": season_number,
                                    "missing_count": missing_count,
                                },
                            )

                            return {
                                "status": "no_sonarr_valid_cached_pack",
                                "season": season_number,
                                "show": show_title,
                                "missing_episodes": missing_count,
                                "message": (
                                    "Aucune release validée par Sonarr et confirmée "
                                    "cache AllDebrid. Épisodes conservés."
                                ),
                            }

                    await manager.send_enhanced_progress_update(
                        self.user_id,
                        show_title,
                        "season_it_single",
                        f"🗑️ Préparation de la suppression de {missing_count} épisode(s)...",
                        45,
                        current_step="Préparation suppression",
                        details={
                            "poster_url": poster_url,
                            "season_number": season_number,
                            "missing_count": missing_count,
                        },
                    )

                    await manager.send_enhanced_progress_update(
                        self.user_id,
                        show_title,
                        "season_it_single",
                        f"🧹 Suppression des épisodes existants de la saison {season_number}...",
                        50,
                        current_step="Suppression",
                        details={
                            "poster_url": poster_url,
                            "season_number": season_number,
                            "missing_count": missing_count,
                        },
                    )

                    logger.info(
                        "SeasonIt - Suppression des anciens épisodes de %s S%s",
                        show_title,
                        season_number,
                    )

                    await client.delete_season_episodes(show_id, season_number)

                    await manager.send_enhanced_progress_update(
                        self.user_id,
                        show_title,
                        "season_it_single",
                        "✅ Suppression des épisodes terminée avec succès...",
                        55,
                        current_step="Confirmation suppression",
                        details={
                            "poster_url": poster_url,
                            "season_number": season_number,
                            "missing_count": missing_count,
                        },
                    )

                    if require_cached_pack_before_deletion and selected_release:
                        logger.info(
                            "SeasonIt - Téléchargement direct lancé : %s",
                            selected_release.get("title", selected_release.get("guid")),
                        )

                        download_result = await client.download_release_direct(
                            selected_release["guid"],
                            int(selected_release["indexerId"]),
                        )

                        await manager.send_enhanced_progress_update(
                            self.user_id,
                            show_title,
                            "season_it_single",
                            "✅ Release validée/cache envoyée au téléchargement direct",
                            100,
                            "success",
                            current_step="Téléchargement direct",
                            details={
                                "poster_url": poster_url,
                                "season_number": season_number,
                                "missing_count": missing_count,
                                "release_title": selected_release.get("title"),
                            },
                        )

                        return {
                            "status": "success_direct_sonarr_valid_cached_pack",
                            "season": season_number,
                            "show": show_title,
                            "missing_episodes": missing_count,
                            "release": {
                                "title": selected_release.get("title"),
                                "guid": selected_release.get("guid"),
                                "indexerId": selected_release.get("indexerId"),
                                "customFormatScore": selected_release.get("customFormatScore"),
                                "quality": selected_release.get("quality"),
                            },
                            "download_result": download_result,
                            "message": (
                                "Release validée par Sonarr et confirmée cache AllDebrid. "
                                "Épisodes supprimés puis release téléchargée directement."
                            ),
                        }

        # ------------------------------------------------------------------
        # Fallback / legacy path: regular SeasonSearch
        # ------------------------------------------------------------------
        if require_cached_pack_before_deletion:
            logger.warning(
                "SeasonIt - Recherche Sonarr automatique ignorée : cache AllDebrid requis avant suppression pour %s S%s",
                show_title,
                season_number,
            )

            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "season_it_single",
                "🛡️ Recherche fallback Sonarr ignorée : cache AllDebrid requis",
                100,
                "warning",
                current_step="Arrêt sécurisé",
                details={
                    "poster_url": poster_url,
                    "season_number": season_number,
                    "missing_count": missing_count,
                },
            )

            return {
                "status": "safe_stop_no_fallback",
                "season": season_number,
                "show": show_title,
                "missing_episodes": missing_count,
                "message": (
                    "Recherche fallback Sonarr ignorée car require_cached_pack_before_deletion=True. "
                    "Aucune recherche épisode par épisode déclenchée."
                ),
            }

        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            f"🔧 Validation des paramètres de recherche pour la saison {season_number}...",
            65,
            current_step="Validation paramètres",
            details={
                "poster_url": poster_url,
                "season_number": season_number,
                "missing_count": missing_count,
            },
        )

        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            "🚀 Envoi de la demande de recherche saison à Sonarr...",
            70,
            current_step="Envoi recherche",
            details={
                "poster_url": poster_url,
                "season_number": season_number,
                "missing_count": missing_count,
            },
        )

        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            "⏳ Sonarr traite la demande de recherche saison...",
            80,
            current_step="Traitement recherche",
            details={
                "poster_url": poster_url,
                "season_number": season_number,
                "missing_count": missing_count,
            },
        )

        command_id = await client.search_season_pack(show_id, season_number)

        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            f"✅ Commande de recherche exécutée avec succès (ID : {command_id})...",
            90,
            current_step="Vérification commande",
            details={
                "poster_url": poster_url,
                "season_number": season_number,
                "command_id": command_id,
            },
        )

        await manager.send_enhanced_progress_update(
            self.user_id,
            show_title,
            "season_it_single",
            f"🎉 Season It terminé ! {show_title} saison {season_number} est maintenant traité par Sonarr.",
            100,
            "success",
            current_step="Terminé",
            details={
                "poster_url": poster_url,
                "season_number": season_number,
                "command_id": command_id,
            },
        )

        return {
            "status": "success",
            "season": season_number,
            "show": show_title,
            "missing_episodes": missing_count,
            "command_id": command_id,
            "message": f"Recherche saison déclenchée avec succès pour la saison {season_number}",
        }

    async def _process_all_seasons(self, client: SonarrClient, show_id: int, show_title: str, series_data: Dict) -> Dict[str, Any]:
        """Traitement détaillé de toutes les saisons avec suivi de progression"""
        
        # Step 1: Initialize all seasons process
        poster_url = client._get_poster_url(series_data.get("images", []), client.instance_id)
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"🔄 Initialisation de Season It pour toutes les saisons de '{show_title}'...", 
            5,
            current_step="Initialisation",
            details={"poster_url": poster_url}
        )
        
        # Step 2: Scan all seasons for missing episodes
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"🔍 Recherche des épisodes manquants dans toutes les saisons...", 
            10,
            current_step="Analyse des épisodes",
            details={"poster_url": poster_url}
        )
        
        missing_data = await client.get_missing_episodes(show_id)
        seasons_with_missing = missing_data.get("seasons_with_missing", {})
        
        if not seasons_with_missing:
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_all",
                f"✅ Aucun épisode manquant trouvé pour '{show_title}'", 
                100, 
                "warning",
                current_step="Terminé",
                details={"poster_url": poster_url}
            )
            return {"status": "no_missing_episodes", "message": "Aucun épisode manquant trouvé"}

        # Step 3: Check for future episodes
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"📅 Vérification des saisons avec épisodes non diffusés...", 
            12,
            current_step="Vérification épisodes futurs",
            details={"poster_url": poster_url}
        )
        
        future_check = await client.has_future_episodes(show_id)
        complete_seasons = set(future_check.get("seasons_complete", []))
        
        # Step 4: Analyze series structure
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"   Analyse de la structure de la série et des saisons surveillées...", 
            15,
            current_step="Analyse structure",
            details={"poster_url": poster_url}
        )
        
        seasons = series_data.get("seasons", [])
        monitored_seasons = [s for s in seasons if s.get("monitored", False) and s.get("seasonNumber", 0) > 0]
        # Filter out seasons with missing episodes AND seasons that have future episodes
        seasons_to_process = [s for s in monitored_seasons if s["seasonNumber"] in seasons_with_missing and s["seasonNumber"] in complete_seasons]
        
        if not seasons_to_process:
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_all",
                f"⚠️ Aucune saison complète avec épisodes manquants trouvée pour '{show_title}' (les saisons incomplètes avec épisodes futurs sont exclues)", 
                100, 
                "warning",
                current_step="Terminé",
                details={"poster_url": poster_url}
            )
            return {"status": "no_seasons_to_process"}

        # Step 4: Calculate processing scope
        total_missing = sum(len(episodes) for episodes in seasons_with_missing.values())
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"📈 Périmètre du traitement : {total_missing} épisode(s) manquant(s) sur {len(seasons_to_process)} saison(s)", 
            20,
            current_step="Calcul périmètre",
            details={"poster_url": poster_url, "total_missing": total_missing, "seasons_count": len(seasons_to_process)}
        )

        # Step 5: Initialize processing queue
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"🎯 Préparation du traitement séquentiel de {len(seasons_to_process)} saison(s)...", 
            25,
            current_step="Préparation file",
            details={"poster_url": poster_url, "seasons_count": len(seasons_to_process)}
        )

        results = []
        total_seasons = len(seasons_to_process)
        
        for i, season in enumerate(seasons_to_process):
            season_num = season["seasonNumber"]
            season_missing = len(seasons_with_missing.get(season_num, []))
            base_progress = 30 + (i * 60 // total_seasons)
            
            try:
                # Step 6+: Process individual seasons
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_all",
                    f"🎬 Démarrage de la saison {season_num} ({i+1}/{total_seasons}) - {season_missing} épisode(s) manquant(s)", 
                    base_progress,
                    current_step=f"Traitement saison {season_num}",
                    details={"poster_url": poster_url, "season_number": season_num, "current_season": i+1, "total_seasons": total_seasons}
                )
                
                # Get poster URL for this show
                poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)
                result = await self._process_single_season(client, show_id, season_num, show_title, poster_url, series_data)
                results.append(result)
                
                # Progress update after each season
                completed_progress = 30 + ((i + 1) * 60 // total_seasons)
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_all",
                    f"✅ Saison {season_num} terminée ({i+1}/{total_seasons})", 
                    completed_progress,
                    current_step=f"Saison {season_num} terminée",
                    details={"poster_url": poster_url, "season_number": season_num, "current_season": i+1, "total_seasons": total_seasons}
                )
                
                # Add delay between seasons to avoid overwhelming Sonarr
                if i < len(seasons_to_process) - 1:
                    await manager.send_enhanced_progress_update(
                        self.user_id, 
                        show_title,
                        "season_it_all",
                        f"⏳ Attente de 3 secondes avant la saison suivante...", 
                        completed_progress + 1,
                        current_step="Attente",
                        details={"poster_url": poster_url}
                    )
                    await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"Error processing season {season_num}: {e}")
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_all",
                    f"❌ Échec de la saison {season_num} : {str(e)}", 
                    base_progress + 5,
                    "error",
                    current_step=f"Saison {season_num} échouée",
                    details={"poster_url": poster_url, "season_number": season_num, "error": str(e)}
                )
                results.append({
                    "status": "error",
                    "season": season_num,
                    "error": str(e)
                })

        # Final analysis and reporting
        successful_seasons = [r for r in results if r.get("status") == "success"]
        failed_seasons = [r for r in results if r.get("status") == "error"]
        
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"📊 Traitement terminé : {len(successful_seasons)} réussite(s), {len(failed_seasons)} échec(s)", 
            95,
            current_step="Traitement terminé",
            details={"poster_url": poster_url, "successful_count": len(successful_seasons), "failed_count": len(failed_seasons)}
        )
        
        # Final completion message
        if failed_seasons:
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_all",
                f"⚠️ Season It terminé avec résultats partiels pour '{show_title}' : {len(successful_seasons)}/{len(results)} saison(s) réussie(s)", 
                100, 
                "warning",
                current_step="Terminé",
                details={"poster_url": poster_url, "successful_count": len(successful_seasons), "failed_count": len(failed_seasons)}
            )
        else:
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_all",
                f"🎉 Season It terminé avec succès pour les {len(successful_seasons)} saison(s) de '{show_title}' !", 
                100, 
                "success",
                current_step="Terminé",
                details={"poster_url": poster_url, "successful_count": len(successful_seasons)}
            )

        return {
            "status": "completed",
            "show": show_title,
            "total_missing_episodes": total_missing,
            "processed_seasons": len(results),
            "successful_seasons": len(successful_seasons),
            "results": results
        }

    def _get_sonarr_instance(self, show_id: int) -> Optional[SonarrInstance]:
        return self.db.query(SonarrInstance).filter(
            SonarrInstance.owner_id == self.user_id,
            SonarrInstance.is_active == True
        ).first()
    
    def _get_sonarr_instance_by_id(self, instance_id: int) -> Optional[SonarrInstance]:
        return self.db.query(SonarrInstance).filter(
            SonarrInstance.id == instance_id,
            SonarrInstance.owner_id == self.user_id,
            SonarrInstance.is_active == True
        ).first()

    async def _get_series_data(self, client: SonarrClient, show_id: int) -> Dict[str, Any]:
        import httpx
        async with httpx.AsyncClient() as http_client:
            response = await http_client.get(
                f"{client.base_url}/api/v3/series/{show_id}",
                headers=client.headers
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to fetch series data: {response.status_code}")
            
            return response.json()

    async def process_bulk_season_it(self, show_items: List[Dict]) -> Dict[str, Any]:
        """Process Season It for multiple shows using bulk operation manager"""
        operation_id = bulk_operation_manager.create_operation(
            user_id=self.user_id,
            operation_type="season_it_bulk",
            items=show_items,
            operation_func=self._process_bulk_item,
            description=f"Season It bulk operation for {len(show_items)} shows"
        )
        
        return await bulk_operation_manager.execute_operation(operation_id)
    
    async def _process_bulk_item(self, item: Dict, progress_callback: callable) -> Dict[str, Any]:
        """Traite un élément dans une opération bulk"""
        show_id = item.get('id')
        show_title = item.get('name', f"Show {show_id}")
        season_number = item.get('season_number')  # None for all seasons
        
        try:
            # Get Sonarr instance
            instance_id = item.get('instance_id')
            instance = self._get_sonarr_instance_by_id(instance_id) if instance_id else self._get_sonarr_instance(show_id)
            if not instance:
                raise Exception("Aucune instance Sonarr trouvée pour cette série")
                
            client = SonarrClient(instance.url, instance.api_key, instance.id)
            
            # Get series data
            series_data = await self._get_series_data(client, show_id)
            show_title = series_data.get("title", show_title)
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)
            
            await progress_callback(10, f"Démarrage de Season It pour {show_title}", poster_url)
            
            await progress_callback(25, f"Traitement de {show_title}", poster_url)
            
            # Create activity log
            activity = self._create_activity_log(instance.id, show_id, show_title, season_number)
            
            if season_number:
                result = await self._process_single_season_with_callback(
                    client, show_id, season_number, show_title, progress_callback, poster_url
                )
            else:
                result = await self._process_all_seasons_with_callback(
                    client, show_id, show_title, series_data, progress_callback, poster_url
                )
            
            # Update activity log on success
            self._update_activity_log(
                activity,
                "success",
                f"Season It terminé avec succès pour {show_title}" + (f" saison {season_number}" if season_number else " toutes saisons")
            )
            
            await progress_callback(100, f"Season It terminé pour {show_title}", poster_url)
            
            return {
                'status': 'success',
                'show_title': show_title,
                'result': result
            }
            
        except Exception as e:
            logger.error(f"Erreur pendant le traitement bulk de {show_title} : {e}")
            if 'activity' in locals():
                self._update_activity_log(
                    activity,
                    "error",
                    f"Season It a échoué pour {show_title}",
                    str(e)
                )
            raise Exception(f"Échec du traitement de {show_title} : {str(e)}")
    
    async def _process_single_season_with_callback(self, client: SonarrClient, show_id: int,
                                                  season_number: int, show_title: str,
                                                  progress_callback: callable, poster_url: str = None) -> Dict[str, Any]:
        """Traite une saison avec callback de progression pour les opérations bulk"""
        await progress_callback(
            25,
            f"Vérification des épisodes futurs pour {show_title} saison {season_number}",
            poster_url,
        )

        future_check = await client.has_future_episodes(show_id, season_number)

        if season_number in future_check.get("seasons_incomplete", []):
            await progress_callback(
                100,
                f"{show_title} saison {season_number} contient des épisodes non diffusés : traitement ignoré",
                poster_url,
            )

            return {
                "status": "incomplete_season",
                "message": "La saison contient des épisodes non diffusés",
            }

        await progress_callback(
            30,
            f"Recherche des épisodes manquants pour {show_title} saison {season_number}",
            poster_url,
        )

        missing_data = await client.get_missing_episodes(show_id, season_number)
        seasons_with_missing = missing_data.get("seasons_with_missing", {})

        if season_number not in seasons_with_missing:
            await progress_callback(
                100,
                f"{show_title} saison {season_number} n'a aucun épisode manquant",
                poster_url,
            )

            return {
                "status": "no_missing_episodes",
                "message": "Aucun épisode manquant trouvé",
            }

        missing_count = len(seasons_with_missing[season_number])

        ssd_config = config_manager.config

        disable_season_pack_check = bool(
            getattr(ssd_config, "disable_season_pack_check", False)
        )

        skip_episode_deletion = bool(
            getattr(ssd_config, "skip_episode_deletion", False)
        )

        require_cached_pack_before_deletion = bool(
            getattr(ssd_config, "require_cached_pack_before_deletion", False)
        )

        logger.info(
            f"Paramètres SeasonIt bulk pour {show_title} S{season_number} : "
            f"skip_episode_deletion={skip_episode_deletion}, "
            f"disable_season_pack_check={disable_season_pack_check}, "
            f"require_cached_pack_before_deletion={require_cached_pack_before_deletion}"
        )

        if disable_season_pack_check:
            await progress_callback(
                50,
                f"Recherche de packs saison désactivée pour {show_title}, poursuite avec une recherche classique",
                poster_url,
            )

            if not skip_episode_deletion:
                if require_cached_pack_before_deletion:
                    logger.warning(
                        "Bulk suppression annulée : require_cached_pack_before_deletion=True "
                        "mais la recherche de packs saison est désactivée pour %s S%s",
                        show_title,
                        season_number,
                    )

                    await progress_callback(
                        100,
                        f"Suppression annulée pour {show_title} : cache pack requis mais recherche pack désactivée",
                        poster_url,
                    )

                    return {
                        "status": "safe_stop_pack_check_disabled",
                        "season": season_number,
                        "show": show_title,
                        "missing_episodes": missing_count,
                        "message": (
                            "Suppression annulée : la vérification du cache pack est requise, "
                            "mais la recherche de packs saison est désactivée."
                        ),
                    }

                await progress_callback(
                    60,
                    f"Suppression des épisodes individuels de {show_title}",
                    poster_url,
                )

                await client.delete_season_episodes(show_id, season_number)

        else:
            await progress_callback(
                40,
                f"Recherche de packs saison pour {show_title}",
                poster_url,
            )

            releases = await client._get_releases(
                show_id,
                season_number,
                show_title,
            )

            logger.info(
                "Recherche Sonarr bulk : %s release(s) trouvée(s) pour %s S%s",
                len(releases),
                show_title,
                season_number,
            )

            if not releases:
                await progress_callback(
                    60,
                    f"Aucun pack saison trouvé pour {show_title}, poursuite avec une recherche classique",
                    poster_url,
                )

            else:
                if not skip_episode_deletion:
                    selected_release = None

                    if require_cached_pack_before_deletion:
                        await progress_callback(
                            65,
                            f"Vérification release Sonarr + cache AllDebrid pour {show_title}",
                            poster_url,
                        )

                        selected_release = await self._find_sonarr_valid_cached_release(
                            releases
                        )

                        if not selected_release:
                            logger.warning(
                                "Bulk suppression annulée : aucune release validée par Sonarr "
                                "et confirmée cache AllDebrid pour %s S%s",
                                show_title,
                                season_number,
                            )

                            await progress_callback(
                                100,
                                f"Aucun pack validé Sonarr + cache trouvé pour {show_title} ; suppression annulée",
                                poster_url,
                            )

                            return {
                                "status": "no_sonarr_valid_cached_pack",
                                "season": season_number,
                                "show": show_title,
                                "missing_episodes": missing_count,
                                "message": "Suppression annulée : aucun pack validé par Sonarr et confirmé cache trouvé",
                            }

                    await progress_callback(
                        70,
                        f"Suppression des épisodes individuels de {show_title}",
                        poster_url,
                    )

                    await client.delete_season_episodes(show_id, season_number)

                    if require_cached_pack_before_deletion and selected_release:
                        download_result = await client.download_release_direct(
                            selected_release["guid"],
                            int(selected_release["indexerId"]),
                        )

                        return {
                            "status": "success_direct_sonarr_valid_cached_pack",
                            "season": season_number,
                            "show": show_title,
                            "missing_episodes": missing_count,
                            "release": {
                                "title": selected_release.get("title"),
                                "guid": selected_release.get("guid"),
                                "indexerId": selected_release.get("indexerId"),
                                "customFormatScore": selected_release.get("customFormatScore"),
                            },
                            "download_result": download_result,
                        }

        if require_cached_pack_before_deletion:
            logger.warning(
                "SEASONIT DEBUG - Bulk fallback Sonarr ignoré pour %s S%s : "
                "require_cached_pack_before_deletion=True",
                show_title,
                season_number,
            )

            await progress_callback(
                100,
                f"Recherche fallback Sonarr ignorée pour {show_title} : cache AllDebrid requis",
                poster_url,
            )

            return {
                "status": "safe_stop_no_fallback",
                "season": season_number,
                "show": show_title,
                "missing_episodes": missing_count,
                "message": (
                    "Recherche fallback Sonarr ignorée car require_cached_pack_before_deletion=True. "
                    "Aucune recherche épisode par épisode déclenchée."
                ),
            }

        await progress_callback(
            80,
            f"Déclenchement de la recherche saison pour {show_title}",
            poster_url,
        )

        command_id = await client.search_season_pack(show_id, season_number)

        return {
            "status": "success",
            "season": season_number,
            "show": show_title,
            "missing_episodes": missing_count,
            "command_id": command_id,
        }
    
    async def _process_all_seasons_with_callback(self, client: SonarrClient, show_id: int, 
                                               show_title: str, series_data: Dict, 
                                               progress_callback: callable, poster_url: str = None) -> Dict[str, Any]:
        """Traite toutes les saisons avec callback de progression pour les opérations bulk"""
        await progress_callback(25, f"Vérification des épisodes futurs", poster_url)
        
        # Check for future episodes first
        future_check = await client.has_future_episodes(show_id)
        complete_seasons = set(future_check.get("seasons_complete", []))
        
        await progress_callback(30, f"Recherche des épisodes manquants dans toutes les saisons", poster_url)
        
        missing_data = await client.get_missing_episodes(show_id)
        seasons_with_missing = missing_data.get("seasons_with_missing", {})
        
        if not seasons_with_missing:
            await progress_callback(100, f"Aucun épisode manquant trouvé", poster_url)
            return {"status": "no_missing_episodes", "message": "Aucun épisode manquant trouvé"}
        
        seasons = series_data.get("seasons", [])
        monitored_seasons = [s for s in seasons if s.get("monitored", False) and s.get("seasonNumber", 0) > 0]
        # Filter out seasons with missing episodes AND seasons that have future episodes
        seasons_to_process = [s for s in monitored_seasons if s["seasonNumber"] in seasons_with_missing and s["seasonNumber"] in complete_seasons]
        
        if not seasons_to_process:
            await progress_callback(100, f"Aucune saison complète avec épisodes manquants à traiter", poster_url)
            return {"status": "no_seasons_to_process"}
        
        results = []
        total_seasons = len(seasons_to_process)
        
        for i, season in enumerate(seasons_to_process):
            season_num = season["seasonNumber"]
            base_progress = 40 + (i * 50 // total_seasons)
            
            await progress_callback(base_progress, f"Traitement de la saison {season_num} ({i+1}/{total_seasons})", poster_url)
            
            try:
                result = await self._process_single_season_with_callback(
                    client, show_id, season_num, show_title, 
                    lambda p, m, poster=None: progress_callback(base_progress + (p * 50 // (total_seasons * 100)), m, poster_url),
                    poster_url
                )
                results.append(result)
                
                if i < len(seasons_to_process) - 1:
                    await asyncio.sleep(3)
                    
            except Exception as e:
                logger.error(
                    f"Erreur pendant le traitement de {show_title} S{season_num} : {e}"
                )
                results.append({
                    "status": "error",
                    "season": season_num,
                    "error": str(e)
                })
        
        successful_seasons = [r for r in results if r.get("status") == "success"]
        
        return {
            "status": "completed",
            "show": show_title,
            "processed_seasons": len(results),
            "successful_seasons": len(successful_seasons),
            "results": results
        }

    async def search_season_packs_interactive(self, show_id: int, season_number: int, instance_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Recherche les packs saison et retourne les résultats formatés pour la sélection interactive"""
        import asyncio

        try:
            # Get series data first
            instance = self._get_sonarr_instance_by_id(instance_id) if instance_id else self._get_sonarr_instance(show_id)
            if not instance:
                raise Exception("Aucune instance Sonarr trouvée pour cette série")

            client = SonarrClient(instance.url, instance.api_key, instance.id)
            series_data = await self._get_series_data(client, show_id)
            show_title = series_data.get("title", "Unknown Show")
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)

            # Check for cancellation
            if asyncio.current_task().cancelled():
                logger.info(f"🚫 Recherche annulée pour {show_title} saison {season_number}")

                await manager.send_personal_message({
                    "type": "clear_progress",
                    "operation_type": "interactive_search",
                    "message": "🚫 Recherche annulée par l'utilisateur"
                }, self.user_id)

                raise asyncio.CancelledError()

            # Send search start notification
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "interactive_search",
                f"🔍 Recherche des releases de la saison {season_number}...",
                20,
                current_step="Recherche",
                details={"poster_url": poster_url, "season_number": season_number}
            )

            # Search for releases from Sonarr
            releases = await client._get_releases(show_id, season_number)

            logger.info(
                "Recherche interactive Sonarr : %s release(s) trouvée(s) pour %s S%s",
                len(releases),
                show_title,
                season_number,
            )

            # Check for cancellation after the slow API call
            if asyncio.current_task().cancelled():
                logger.info(f"🚫 Recherche annulée pour {show_title} saison {season_number} après l'appel API")

                await manager.send_personal_message({
                    "type": "clear_progress",
                    "operation_type": "interactive_search",
                    "message": "🚫 Recherche annulée par l'utilisateur"
                }, self.user_id)

                raise asyncio.CancelledError()

            # Send results found notification
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "interactive_search",
                f"📊 {len(releases)} release(s) pack saison trouvée(s)",
                70,
                current_step="Traitement des résultats",
                details={
                    "poster_url": poster_url,
                    "season_number": season_number,
                    "releases_found": len(releases)
                }
            )

            # Format releases for frontend
            formatted_releases = []

            logger.info(
                f"Formatage de {len(releases)} release(s) pour l'interface de {show_title} S{season_number}"
            )

            for i, release in enumerate(releases):

                formatted_release = self._format_release_for_ui(release)

                logger.info(
                    "Release formatée %s pour %s S%s : score_qualité=%s indexer_id=%s approuvée=%s titre=%s",
                    i + 1,
                    show_title,
                    season_number,
                    formatted_release.get("quality_score", "NOT_FOUND"),
                    formatted_release.get("indexer_id", "NOT_FOUND"),
                    formatted_release.get("approved", "NOT_FOUND"),
                    formatted_release.get("title", "NOT_FOUND"),
                )

                formatted_releases.append(formatted_release)

            # Sort releases by quality and seeders
            formatted_releases.sort(
                key=lambda x: (
                    -int(x.get("release_weight") or 0),
                    -int(x.get("seeders") or 0),
                )
            )

            # Send completion notification
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "interactive_search",
                f"✅ Recherche interactive terminée : {len(formatted_releases)} release(s) prête(s) pour la sélection",
                100,
                "success",
                current_step="Terminé",
                details={
                    "poster_url": poster_url,
                    "season_number": season_number,
                    "releases_found": len(formatted_releases)
                }
            )

            return formatted_releases

        except Exception as e:
            logger.error(f"Erreur pendant la recherche interactive : {e}")

            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title if 'show_title' in locals() else "Unknown Show",
                "interactive_search",
                f"❌ Échec de la recherche : {str(e)}",
                100,
                "error",
                current_step="Erreur",
                details={"error": str(e)}
            )

            raise

    async def download_specific_release(self, release_guid: str, show_id: int, season_number: int, instance_id: Optional[int] = None, indexer_id: Optional[int] = None) -> Dict[str, Any]:
        """Télécharge une release précise par GUID"""
        activity = None
        try:
            # Get series data first
            instance = self._get_sonarr_instance_by_id(instance_id) if instance_id else self._get_sonarr_instance(show_id)
            if not instance:
                raise Exception("Aucune instance Sonarr trouvée pour cette série")

            client = SonarrClient(instance.url, instance.api_key, instance.id)
            series_data = await self._get_series_data(client, show_id)
            show_title = series_data.get("title", "Unknown Show")
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)

            # Create activity log
            activity = self._create_activity_log(instance.id, show_id, show_title, season_number)

            # Send download start notification
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "manual_download",
                f"🚀 Démarrage du téléchargement manuel pour la saison {season_number}...",
                10,
                current_step="Initialisation",
                details={"poster_url": poster_url, "season_number": season_number}
            )

            # Check SSD global settings
            ssd_config = config_manager.config
            skip_deletion = bool(
                getattr(ssd_config, "skip_episode_deletion", False)
            )

            if not skip_deletion:
                # Delete existing episodes
                await manager.send_enhanced_progress_update(
                    self.user_id,
                    show_title,
                    "manual_download",
                    f"🗑️ Suppression des épisodes existants de la saison {season_number}...",
                    30,
                    current_step="Suppression épisodes",
                    details={"poster_url": poster_url, "season_number": season_number}
                )
                
                await client.delete_season_episodes(show_id, season_number)

            # Download the specific release
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "manual_download",
                f"📥 Téléchargement de la release sélectionnée...",
                60,
                current_step="Téléchargement",
                details={"poster_url": poster_url, "season_number": season_number}
            )

            # Use Sonarr's download API with the provided indexer_id
            download_result = await client.download_release_direct(release_guid, indexer_id)

            # Complete notification
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "manual_download",
                f"✅ Téléchargement lancé avec succès pour la saison {season_number}",
                100,
                "success",
                current_step="Terminé",
                details={"poster_url": poster_url, "season_number": season_number}
            )

            # Update activity log
            self._update_activity_log(
                activity,
                "success",
                f"Téléchargement manuel terminé pour {show_title} saison {season_number}"
            )

            return {
                "status": "success",
                "show": show_title,
                "season": season_number,
                "message": "Téléchargement lancé avec succès"
            }

        except Exception as e:
            logger.error(f"Erreur pendant le téléchargement manuel de la release : {e}")
            
            # Update activity log on error
            if activity:
                self._update_activity_log(
                    activity,
                    "error",
                    f"Échec du téléchargement manuel pour {activity.show_title}",
                    str(e)
                )

            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title if 'show_title' in locals() else "Unknown Show",
                "manual_download",
                f"❌ Échec du téléchargement : {str(e)}",
                100,
                "error",
                current_step="Erreur",
                details={"error": str(e)}
            )
            raise

    def _format_release_for_ui(self, release: Dict[str, Any]) -> Dict[str, Any]:
        """Formate une release pour l'affichage frontend"""
        # Helper function to format file size
        def format_size(size_bytes):
            if size_bytes >= 1024**3:
                return f"{size_bytes / (1024**3):.1f} GB"
            elif size_bytes >= 1024**2:
                return f"{size_bytes / (1024**2):.1f} MB"
            elif size_bytes >= 1024:
                return f"{size_bytes / 1024:.1f} KB"
            else:
                return f"{size_bytes} B"

        # Helper function to format age
        def format_age(age_hours):
            if age_hours < 1:
                return "< 1 hour"
            elif age_hours < 24:
                return f"{int(age_hours)} hours"
            elif age_hours < 24 * 7:
                return f"{int(age_hours / 24)} days"
            elif age_hours < 24 * 30:
                return f"{int(age_hours / (24 * 7))} weeks"
            elif age_hours < 24 * 365:
                return f"{int(age_hours / (24 * 30))} months"
            else:
                # Convert to years and months
                total_months = int(age_hours / (24 * 30))
                years = total_months // 12
                months = total_months % 12
                if months == 0:
                    return f"{years} years"
                else:
                    return f"{years} years {months} months"

        # Extract quality information
        quality = release.get("quality", {})
        
        # Sonarr release structure typically has:
        # quality: { quality: { id: 6, name: "WEBDL-1080p" } }
        # And quality score should be at the top level as "qualityScore" or "score"
        
        if "quality" in quality:
            # Nested structure (most common)
            quality_info = quality.get("quality", {})
            quality_name = quality_info.get("name", "Unknown")
        else:
            # Direct structure (fallback)
            quality_name = quality.get("name", "Unknown")
        
        # Quality score extraction using customFormatScore from Sonarr API
        # This includes both positive and negative scores (negative = rejected/poor quality)
        quality_score = release.get("customFormatScore", 0)
        logger.debug(
            f"Release : {release.get('title', 'Unknown')[:50]}... - scoreCustomFormat : {quality_score}"
        )        
        # Calculate release weight for sorting (higher = better)
        release_weight = quality_score
        
        # Bonus for proper/repack
        if release.get("proper", False):
            release_weight += 50
        if "repack" in release.get("title", "").lower():
            release_weight += 25

        return {
            "guid": release.get("guid", ""),
            "title": release.get("title", "Unknown"),
            "size": release.get("size", 0),
            "size_formatted": format_size(release.get("size", 0)),
            "seeders": release.get("seeders", 0),
            "leechers": release.get("leechers", 0),
            "age": release.get("age", 0),
            "age_formatted": format_age(release.get("ageHours", 0)),  # Use ageHours directly
            "quality": quality_name,
            "quality_score": quality_score,
            "indexer": release.get("indexer", "Unknown"),
            "indexer_id": release.get("indexerId", 0),  # Store indexerId for download
            "approved": release.get("approved", True),
            "indexer_flags": release.get("indexerFlags", []),
            "release_weight": release_weight
        }

