import asyncio
import logging
import json
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from integrations.seasonarr.db.models import SonarrInstance, UserSettings, ActivityLog
from integrations.seasonarr.clients.sonarr_client import SonarrClient
from integrations.seasonarr.core.websocket_manager import manager
from integrations.seasonarr.services.bulk_operation_manager import bulk_operation_manager
from datetime import datetime

logger = logging.getLogger(__name__)

class SeasonItService:
    def __init__(self, db: Session, user_id: int):
        self.db = db
        self.user_id = user_id

    def _create_activity_log(self, instance_id: int, show_id: int, show_title: str, season_number: Optional[int] = None) -> ActivityLog:
        """Créer une nouvelle entrée dans le journal d’activité"""
        activity = ActivityLog(
            user_id=self.user_id,
            instance_id=instance_id,
            action_type="season_it",
            show_id=show_id,
            show_title=show_title,
            season_number=season_number,
            status="in_progress",
            message=f"🚀 Début de la recherche pour {show_title}" + (f" Saison {season_number}" if season_number else " (Toutes les saisons)")
        )
        self.db.add(activity)
        self.db.commit()
        self.db.refresh(activity)
        return activity

    def _filter_approved_releases(self, releases):
        """Ne garder que les releases approuvées par Sonarr (required terms OK)."""
        releases = [r for r in releases if isinstance(r, dict)]
        total = len(releases)

        filtered = [r for r in releases if r.get("approved", False) is True]
        rejected = total - len(filtered)

        if rejected > 0:
            sample = []
            for r in releases:
                if r.get("approved", False) is True:
                    continue

                rej = r.get("rejections", [])

                # Normaliser en liste
                if isinstance(rej, (str, dict)):
                    rej = [rej]
                elif not isinstance(rej, list):
                    rej = [str(rej)]

                reason = "Unknown rejection"
                if rej:
                    first = rej[0]
                    if isinstance(first, dict):
                        reason = first.get("reason") or first.get("message") or str(first)
                    elif isinstance(first, str):
                        reason = first
                    else:
                        reason = str(first)

                sample.append(reason)
                if len(sample) >= 3:
                    break

            logger.info(f"Filtré {rejected}/{total} releases non approuvées. Exemples : {sample}")

        return filtered

    def _update_activity_log(self, activity: ActivityLog, status: str, message: str = None, error_details: str = None):
        """Mettre à jour une entrée du journal d’activité"""
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
            # Récupérer les données de la série pour inclure l’affiche
            instance = self._get_sonarr_instance_by_id(instance_id) if instance_id else self._get_sonarr_instance(show_id)
            if not instance:
                raise Exception("Aucune instance Sonarr trouvée pour cette série")

            client = SonarrClient(instance.url, instance.api_key, instance.id)
            series_data = await self._get_series_data(client, show_id)
            show_title = series_data.get("title", "Série inconnue")
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)

            # Envoyer une mise à jour de progression améliorée
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_single" if season_number else "season_it_all",
                "🚀 Initialisation du processus Season It...", 
                10,
                current_step="Initialisation",
                details={"poster_url": poster_url, "season_number": season_number}
            )

            # Créer une entrée dans le journal d’activité
            activity = self._create_activity_log(instance.id, show_id, show_title, season_number)

            if season_number:
                result = await self._process_single_season_with_data(client, show_id, season_number, show_title, series_data)
            else:
                result = await self._process_all_seasons(client, show_id, show_title, series_data)

            # Mettre à jour le journal d’activité en cas de succès
            self._update_activity_log(
                activity, 
                "success", 
                f"Saison récupérée avec succès pour {show_title}" + (f" Saison {season_number}" if season_number else " (Toutes les saisons)")
            )
            
            return result

        except Exception as e:
            # Mettre à jour le journal d’activité en cas d’erreur
            if activity:
                self._update_activity_log(
                    activity, 
                    "error", 
                    f"Échec de Season It pour {activity.show_title}",
                    str(e)
                )
            
            await manager.send_enhanced_progress_update(
                self.user_id, 
                activity.show_title if activity else "Série inconnue",
                "season_it_error",
                f"❌ Échec de Season It : {str(e)}", 
                100, 
                "error",
                current_step="Erreur",
                details={"error": str(e)}
            )
            raise

    async def _process_single_season_with_data(self, client: SonarrClient, show_id: int, season_number: int, show_title: str, series_data: Dict) -> Dict[str, Any]:
        """Traitement amélioré d'une saison unique avec suivi détaillé de la progression (15+ étapes)"""
        
        # Récupérer l’URL de l’affiche à partir des données de la série
        poster_url = client._get_poster_url(series_data.get("images", []), client.instance_id)
        
        return await self._process_single_season(client, show_id, season_number, show_title, poster_url, series_data)

    async def _process_single_season(self, client: SonarrClient, show_id: int, season_number: int, show_title: str, poster_url: str = None, series_data: Dict = None) -> Dict[str, Any]:
        """Traitement amélioré d'une saison unique avec suivi détaillé de la progression (15+ étapes)"""
        
        # Si les données de la série ne sont pas fournies, les récupérer
        if series_data is None:
            series_data = await self._get_series_data(client, show_id)
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)
        
        # Étape 1 : Initialiser le processus
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"🔄 Initialisation de Season It pour {show_title} Saison {season_number}...", 
            5,
            current_step="Initialisation",
            details={"poster_url": poster_url, "season_number": season_number}
        )
        
        # Étape 2 : Vérifier la présence d’épisodes futurs
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"📅 Vérification si la Saison {season_number} contient des épisodes non diffusés...", 
            8,
            current_step="Vérification épisodes futurs",
            details={"poster_url": poster_url, "season_number": season_number}
        )
        
        future_check = await client.has_future_episodes(show_id, season_number)
        if season_number in future_check.get("seasons_incomplete", []):
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_single",
                f"⏳ La saison {season_number} de '{show_title}' contient des épisodes qui n’ont pas encore été diffusés. Season It est ignoré afin d’éviter les packs de saison incomplets.", 
                100, 
                "warning",
                current_step="Terminé",
                details={"poster_url": poster_url, "season_number": season_number}
            )
            return {"status": "incomplete_season", "message": "La saison contient des épisodes non diffusés"}
        
        # Étape 3 : Valider les données de la série
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"📋 Validation des données de la série {show_title}...", 
            10,
            current_step="Validation des données",
            details={"poster_url": poster_url, "season_number": season_number}
        )
        
        # Étape 4 : Vérifier les épisodes manquants
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"🔍 Analyse des épisodes manquants dans la Saison {season_number}...", 
            15,
            current_step="Analyse épisodes",
            details={"poster_url": poster_url, "season_number": season_number}
        )

        missing_data = await client.get_missing_episodes(show_id, season_number)
        seasons_with_missing = missing_data.get("seasons_with_missing", {})

        missing_count = len(seasons_with_missing.get(season_number, []))
        episode_nums = [ep.get("episodeNumber") for ep in seasons_with_missing.get(season_number, [])]

        logger.info(
            f"Épisodes manquants pour série {show_id} saison {season_number} : "
            f"{missing_count} épisodes ({episode_nums})"
        )

        if season_number not in seasons_with_missing:
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_single",
                f"✅ La saison {season_number} de '{show_title}' n'a pas d'épisodes manquants", 
                100, 
                "warning",
                current_step="Terminé",
                details={"poster_url": poster_url, "season_number": season_number}
            )
            return {"status": "no_missing_episodes", "message": "Aucun épisode trouvé"}

        missing_count = len(seasons_with_missing[season_number])
        
        # Étape 4 : Charger les paramètres utilisateur
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"⚙️ Chargement des préférences et paramètres utilisateur...", 
            20,
            current_step="Chargement paramètres",
            details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
        )
        
        settings = self.db.query(UserSettings).filter(UserSettings.user_id == self.user_id).first()
        skip_season_pack_check = settings and settings.disable_season_pack_check
        
        # Étape 6 : Déterminer la stratégie de traitement
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"🎯 Détermination de la stratégie de traitement optimale pour {missing_count} épisodes manquants...", 
            25,
            current_step="Stratégie",
            details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
        )
        
        if skip_season_pack_check:
            # Stratégie : ignorer la recherche de pack de saison
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_single",
                f"📝 Vérification des packs de saison désactivée – utilisation de la stratégie de recherche classique…", 
                30,
                current_step="Ignorer Pack Saison",
                details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
            )
            
            if settings and settings.skip_episode_deletion:
                # Étape 7a : Ignorer la suppression d’épisodes
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_single",
                    f"⚠️ Suppression des épisodes ignorée selon les paramètres utilisateur...", 
                    35,
                    current_step="Ignorer Suppression",
                    details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
                )
                logger.info(f"Recherche de pack de saison et suppression d’épisodes ignorées pour la série {show_id} saison {season_number} en raison des paramètres utilisateur")
            else:
                # Étape 7b : Supprimer les épisodes
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_single",
                    f"🗑️ Préparation à la suppression de {missing_count} épisodes individuels…", 
                    35,
                    current_step="Préparer Suppression",
                    details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
                )
                
                # Étape 8 : Exécuter la suppression
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_single",
                    f"🧹 Suppression des épisodes existants de la saison {season_number}...", 
                    40,
                    current_step="Exécuter Suppression",
                    details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
                )
                
                logger.info(f"Suppression des épisodes pour la série {show_id} saison {season_number}")
                await client.delete_season_episodes(show_id, season_number)
                
                # Étape 9 : Confirmer la suppression
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_single",
                    f"✅ Suppression des épisodes terminée avec succès…", 
                    45,
                    current_step="Confirmer Suppression",
                    details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
                )
        else:
            # Stratégie : vérifier d’abord la présence de packs de saison
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_single",
                f"🔍 Recherche de packs de saison disponibles…", 
                30,
                current_step="Recherche Pack Saison",
                details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
            )

            # Étape 7 : Recherche de packs de saison
            releases = await client._get_releases(show_id, season_number)
            logger.info(f"Found {len(releases)} season packs (raw)")

            if not all(isinstance(r, dict) for r in releases):
                logger.error(f"❌ Données inattendues dans releases: {[type(r).__name__ for r in releases]}")
                releases = [r for r in releases if isinstance(r, dict)]

            releases = self._filter_approved_releases(releases)
            logger.info(f"{len(releases)} season packs after approved filter")

            
            # Étape 8 : Analyse des résultats des packs de saison
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_single",
                f"📊 Analyse en cours de {len(releases)} packs de saison disponibles…", 
                35,
                current_step="Analyser Packs Saison",
                current_step_number=7,
                total_steps=17,
                details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count, "releases_found": len(releases)}
            )
            
            if not releases:
                # Étape 9a : Aucun pack de saison trouvé
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_single",
                    f"❌ Aucun pack de saison trouvé – retour à la recherche classique…", 
                    40, 
                    "warning",
                    current_step="Aucun Pack Saison",
                    details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
                )
            else:
                # Étape 9b : Packs de saison trouvés
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_single",
                    f"🎯 Packs de saison adaptés trouvés – préparation à l’optimisation…", 
                    40,
                    current_step="Packs Saison Trouvés",
                    details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count, "releases_found": len(releases)}
                )
                
                if settings and settings.skip_episode_deletion:
                    # Étape 10a : Ignorer suppression avec packs de saison
                    await manager.send_enhanced_progress_update(
                        self.user_id, 
                        show_title,
                        "season_it_single",
                        f"⚠️ Suppression des épisodes ignorée conformément aux paramètres utilisateur…", 
                        45,
                        current_step="Ignorer Suppression",
                        details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
                    )
                    logger.info(f"Suppression des épisodes ignorée pour la série {show_id} saison {season_number} en raison des paramètres utilisateur")
                else:
                    # Étape 10b : Supprimer épisodes avec packs de saison
                    await manager.send_enhanced_progress_update(
                        self.user_id, 
                        show_title,
                        "season_it_single",
                        f"🗑️ Préparation à la suppression de {missing_count} épisodes individuels...", 
                        45,
                        current_step="Préparer Suppression",
                        details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
                    )

                    # Étape 11 : Exécuter la suppression
                    await manager.send_enhanced_progress_update(
                        self.user_id, 
                        show_title,
                        "season_it_single",
                        f"🧹 Suppression des épisodes existants de la saison {season_number}...", 
                        50,
                        current_step="Exécuter Suppression",
                        details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
                    )
                    
                    logger.info(f"Suppression des épisodes existants pour la série {show_id} saison {season_number}")
                    await client.delete_season_episodes(show_id, season_number)

                    # Étape 12 : Confirmer la suppression
                    await manager.send_enhanced_progress_update(
                        self.user_id, 
                        show_title,
                        "season_it_single",
                        f"✅ Suppression des épisodes terminée avec succès...", 
                        55,
                        current_step="Confirmer Suppression",
                        details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
                    )

        # Étape 12 : Préparer la commande de recherche
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"🎬 Préparation de la commande de recherche de saison pour Sonarr...", 
            60,
            current_step="Préparer Recherche",
            details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
        )
        
        # Étape 13 : Valider les paramètres de recherche
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"🔧 Validation des paramètres de recherche pour la saison {season_number}...", 
            65,
            current_step="Valider Paramètres",
            details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
        )
        
        # Étape 14 : Exécuter la commande de recherche
        logger.info(f"Lancement de la recherche de saison pour la série {show_id} saison {season_number}")
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"🚀 Envoi de la requête de recherche de saison à Sonarr...", 
            70,
            current_step="Envoyer Requête Recherche",
            details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
        )
        
        # Étape 15 : Traitement de la requête de recherche
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"⏳ Sonarr traite la requête de recherche de saison...", 
            80,
            current_step="Traitement Recherche",
            details={"poster_url": poster_url, "season_number": season_number, "missing_count": missing_count}
        )
        
        command_id = await client.search_season_pack(show_id, season_number)
        logger.info(f"La commande de recherche de saison {command_id} a été exécutée avec succès")
        
        # Étape 16 : Vérifier l’exécution de la commande
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"✅ Commande de recherche exécutée avec succès (ID : {command_id})...", 
            90,
            current_step="Vérifier Commande",
            details={"poster_url": poster_url, "season_number": season_number, "command_id": command_id}
        )
        
        # Étape 17 : Finaliser le processus
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_single",
            f"🎉 Season It terminé ! {show_title} Saison {season_number} est maintenant en cours de traitement par Sonarr.", 
            100, 
            "success",
            current_step="Terminé",
            details={"poster_url": poster_url, "season_number": season_number, "command_id": command_id}
        )
        
        return {
            "status": "success",
            "season": season_number,
            "show": show_title,
            "missing_episodes": missing_count,
            "command_id": command_id,
            "message": f"Recherche de saison déclenchée avec succès pour la saison {season_number}"
        }

    async def _process_all_seasons(self, client: SonarrClient, show_id: int, show_title: str, series_data: Dict) -> Dict[str, Any]:
        """Traitement amélioré de toutes les saisons avec suivi détaillé de la progression"""
        
        # Étape 1 : Initialiser le processus pour toutes les saisons
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
        
        # Étape 2 : Analyser toutes les saisons pour trouver les épisodes manquants
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"🔍 Analyse de toutes les saisons pour trouver les épisodes manquants...", 
            10,
            current_step="Analyse Épisodes",
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

        # Étape 3 : Vérifier les saisons avec des épisodes non diffusés
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"📅 Vérification des saisons contenant des épisodes non diffusés...", 
            12,
            current_step="Vérification Épisodes Futurs",
            details={"poster_url": poster_url}
        )
        
        future_check = await client.has_future_episodes(show_id)
        complete_seasons = set(future_check.get("seasons_complete", []))
        
        # Étape 4 : Analyser la structure de la série
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"📊 Analyse de la structure de la série et des saisons surveillées...", 
            15,
            current_step="Analyse Structure",
            details={"poster_url": poster_url}
        )
        
        seasons = series_data.get("seasons", [])
        monitored_seasons = [s for s in seasons if s.get("monitored", False) and s.get("seasonNumber", 0) > 0]
        # Filtrer uniquement les saisons manquantes ET complètes (sans épisodes futurs)
        seasons_to_process = [s for s in monitored_seasons if s["seasonNumber"] in seasons_with_missing and s["seasonNumber"] in complete_seasons]
        
        if not seasons_to_process:
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_all",
                f"⚠️ Aucune saison complète avec des épisodes manquants trouvée pour '{show_title}' (les saisons incomplètes avec épisodes futurs sont exclues)", 
                100, 
                "warning",
                current_step="Terminé",
                details={"poster_url": poster_url}
            )
            return {"status": "no_seasons_to_process"}

        # Étape 5 : Calculer l’étendue du traitement
        total_missing = sum(len(episodes) for episodes in seasons_with_missing.values())
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"📈 Portée du traitement : {total_missing} épisodes manquants sur {len(seasons_to_process)} saisons", 
            20,
            current_step="Calcul Portée",
            details={"poster_url": poster_url, "total_missing": total_missing, "seasons_count": len(seasons_to_process)}
        )

        # Étape 6 : Initialiser la file de traitement
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"🎯 Préparation du traitement séquentiel de {len(seasons_to_process)} saisons...", 
            25,
            current_step="Initialiser File",
            details={"poster_url": poster_url, "seasons_count": len(seasons_to_process)}
        )

        results = []
        total_seasons = len(seasons_to_process)
        
        for i, season in enumerate(seasons_to_process):
            season_num = season["seasonNumber"]
            season_missing = len(seasons_with_missing.get(season_num, []))
            base_progress = 30 + (i * 60 // total_seasons)
            
            try:
                # Étape 7+ : Traiter les saisons individuellement
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_all",
                    f"🎬 Démarrage du traitement de la saison {season_num} ({i+1}/{total_seasons}) - {season_missing} épisodes manquants", 
                    base_progress,
                    current_step=f"Traitement Saison {season_num}",
                    details={"poster_url": poster_url, "season_number": season_num, "current_season": i+1, "total_seasons": total_seasons}
                )
                
                # Récupérer l’URL de l’affiche pour cette série
                poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)
                result = await self._process_single_season(client, show_id, season_num, show_title, poster_url, series_data)
                results.append(result)
                
                # Mise à jour de la progression après chaque saison
                completed_progress = 30 + ((i + 1) * 60 // total_seasons)
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_all",
                    f"✅ Saison {season_num} terminée ({i+1}/{total_seasons})", 
                    completed_progress,
                    current_step=f"Saison {season_num} Terminée",
                    details={"poster_url": poster_url, "season_number": season_num, "current_season": i+1, "total_seasons": total_seasons}
                )
                
                # Ajouter un délai entre les saisons pour éviter de surcharger Sonarr
                if i < len(seasons_to_process) - 1:
                    await manager.send_enhanced_progress_update(
                        self.user_id, 
                        show_title,
                        "season_it_all",
                        f"⏳ Attente de 3 secondes avant de traiter la saison suivante...", 
                        completed_progress + 1,
                        current_step="Attente",
                        details={"poster_url": poster_url}
                    )
                    await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement de la saison {season_num} : {e}")
                await manager.send_enhanced_progress_update(
                    self.user_id, 
                    show_title,
                    "season_it_all",
                    f"❌ Échec du traitement de la saison {season_num} : {str(e)}", 
                    base_progress + 5,
                    "error",
                    current_step=f"Saison {season_num} Échec",
                    details={"poster_url": poster_url, "season_number": season_num, "error": str(e)}
                )
                results.append({
                    "status": "error",
                    "season": season_num,
                    "error": str(e)
                })

        # Analyse finale et rapport
        successful_seasons = [r for r in results if r.get("status") == "success"]
        failed_seasons = [r for r in results if r.get("status") == "error"]
        
        await manager.send_enhanced_progress_update(
            self.user_id, 
            show_title,
            "season_it_all",
            f"📊 Traitement terminé : {len(successful_seasons)} réussies, {len(failed_seasons)} échouées", 
            95,
            current_step="Traitement Terminé",
            details={"poster_url": poster_url, "successful_count": len(successful_seasons), "failed_count": len(failed_seasons)}
        )
        
        # Message de fin
        if failed_seasons:
            await manager.send_enhanced_progress_update(
                self.user_id, 
                show_title,
                "season_it_all",
                f"⚠️ Season It terminé avec des résultats mitigés pour '{show_title}' : {len(successful_seasons)}/{len(results)} saisons réussies", 
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
                f"🎉 Season It terminé avec succès pour les {len(successful_seasons)} saisons de '{show_title}' !", 
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
                raise Exception(f"Échec de récupération des données de la série : {response.status_code}")
            
            return response.json()

    async def process_bulk_season_it(self, show_items: List[Dict]) -> Dict[str, Any]:
        """Traiter Season It pour plusieurs séries en utilisant le gestionnaire d’opérations groupées"""
        operation_id = bulk_operation_manager.create_operation(
            user_id=self.user_id,
            operation_type="season_it_bulk",
            items=show_items,
            operation_func=self._process_bulk_item,
            description=f"Opération Season It en lot pour {len(show_items)} séries"
        )
        
        return await bulk_operation_manager.execute_operation(operation_id)
    
    async def _process_bulk_item(self, item: Dict, progress_callback: callable) -> Dict[str, Any]:
        """Traiter un élément unique dans une opération groupée"""
        show_id = item.get('id')
        show_title = item.get('name', f"Série {show_id}")
        season_number = item.get('season_number')  # None pour toutes les saisons
        
        try:
            # Récupérer l’instance Sonarr
            instance_id = item.get('instance_id')
            instance = self._get_sonarr_instance_by_id(instance_id) if instance_id else self._get_sonarr_instance(show_id)
            if not instance:
                raise Exception("Aucune instance Sonarr trouvée pour cette série")
                
            client = SonarrClient(instance.url, instance.api_key, instance.id)
            
            # Récupérer les données de la série
            series_data = await self._get_series_data(client, show_id)
            show_title = series_data.get("title", show_title)
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)
            
            await progress_callback(10, f"Démarrage de Season It pour {show_title}", poster_url)
            
            await progress_callback(25, f"Traitement de {show_title}", poster_url)
            
            # Créer une entrée dans le journal d’activité
            activity = self._create_activity_log(instance.id, show_id, show_title, season_number)
            
            if season_number:
                result = await self._process_single_season_with_callback(
                    client, show_id, season_number, show_title, progress_callback, poster_url
                )
            else:
                result = await self._process_all_seasons_with_callback(
                    client, show_id, show_title, series_data, progress_callback, poster_url
                )
            
            # Mettre à jour le journal d’activité en cas de succès
            self._update_activity_log(
                activity,
                "success",
                f"Season It terminé avec succès pour {show_title}" + (f" Saison {season_number}" if season_number else " (Toutes les saisons)")
            )
            
            await progress_callback(100, f"Season It terminé pour {show_title}", poster_url)
            
            return {
                'status': 'success',
                'show_title': show_title,
                'result': result
            }
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement de l’élément en lot {show_title} : {e}")
            if 'activity' in locals():
                self._update_activity_log(
                    activity,
                    "error",
                    f"Échec de Season It pour {show_title}",
                    str(e)
                )
            raise Exception(f"Échec du traitement de {show_title} : {str(e)}")
    
    async def _process_single_season_with_callback(self, client: SonarrClient, show_id: int, 
                                                  season_number: int, show_title: str, 
                                                  progress_callback: callable, poster_url: str = None) -> Dict[str, Any]:
        """Traiter une saison unique avec suivi de progression pour les opérations groupées"""
        await progress_callback(25, f"Vérification des épisodes futurs dans {show_title} Saison {season_number}", poster_url)
        
        # Vérifier d’abord la présence d’épisodes futurs
        future_check = await client.has_future_episodes(show_id, season_number)
        if season_number in future_check.get("seasons_incomplete", []):
            await progress_callback(100, f"{show_title} Saison {season_number} contient des épisodes non diffusés - ignorée", poster_url)
            return {"status": "incomplete_season", "message": "La saison contient des épisodes qui n’ont pas encore été diffusés"}
        
        await progress_callback(30, f"Vérification des épisodes manquants pour {show_title} Saison {season_number}", poster_url)
        
        missing_data = await client.get_missing_episodes(show_id, season_number)
        seasons_with_missing = missing_data.get("seasons_with_missing", {})
        
        if season_number not in seasons_with_missing:
            await progress_callback(100, f"{show_title} Saison {season_number} n’a pas d’épisodes manquants", poster_url)
            return {"status": "no_missing_episodes", "message": "Aucun épisode manquant trouvé"}
        
        missing_count = len(seasons_with_missing[season_number])
        settings = self.db.query(UserSettings).filter(UserSettings.user_id == self.user_id).first()
        skip_season_pack_check = settings and settings.disable_season_pack_check
        
        if skip_season_pack_check:
            await progress_callback(50, f"Vérification des packs de saison désactivée pour {show_title}, recherche classique en cours", poster_url)
            if not (settings and settings.skip_episode_deletion):
                await progress_callback(60, f"Suppression des épisodes individuels de {show_title}", poster_url)
                await client.delete_season_episodes(show_id, season_number)
        else:
            await progress_callback(40, f"Vérification des packs de saison pour {show_title}", poster_url)
            releases = await client._get_releases(show_id, season_number)

            if not all(isinstance(r, dict) for r in releases):
                logger.error(f"❌ Données inattendues dans releases: {[type(r).__name__ for r in releases]}")
                releases = [r for r in releases if isinstance(r, dict)]

            releases = self._filter_approved_releases(releases)
            logger.info(f"{len(releases)} releases after approved filter")

            
            if not releases:
                await progress_callback(60, f"Aucun pack de saison trouvé pour {show_title}, recherche classique en cours", poster_url)
            else:
                if not (settings and settings.skip_episode_deletion):
                    await progress_callback(70, f"Suppression des épisodes individuels de {show_title}", poster_url)
                    await client.delete_season_episodes(show_id, season_number)
        
        await progress_callback(80, f"Lancement de la recherche de saison pour {show_title}", poster_url)
        command_id = await client.search_season_pack(show_id, season_number)
        
        return {
            "status": "success",
            "season": season_number,
            "show": show_title,
            "missing_episodes": missing_count,
            "command_id": command_id
        }
    
    async def _process_all_seasons_with_callback(self, client: SonarrClient, show_id: int, 
                                               show_title: str, series_data: Dict, 
                                               progress_callback: callable, poster_url: str = None) -> Dict[str, Any]:
        """Traiter toutes les saisons avec suivi de progression pour les opérations groupées"""
        await progress_callback(25, f"Vérification des épisodes futurs", poster_url)
        
        # Vérifier d’abord la présence d’épisodes futurs
        future_check = await client.has_future_episodes(show_id)
        complete_seasons = set(future_check.get("seasons_complete", []))
        
        await progress_callback(30, f"Vérification des épisodes manquants dans toutes les saisons", poster_url)
        
        missing_data = await client.get_missing_episodes(show_id)
        seasons_with_missing = missing_data.get("seasons_with_missing", {})
        
        if not seasons_with_missing:
            await progress_callback(100, f"Aucun épisode manquant trouvé", poster_url)
            return {"status": "no_missing_episodes", "message": "Aucun épisode trouvé"}
        
        seasons = series_data.get("seasons", [])
        monitored_seasons = [s for s in seasons if s.get("monitored", False) and s.get("seasonNumber", 0) > 0]
        # Filtrer uniquement les saisons avec des épisodes manquants ET complètes (pas d’épisodes futurs)
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
                logger.error(f"Erreur lors du traitement de la saison {season_num} : {e}")
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
        """Rechercher des packs de saison et retourner les résultats formatés pour sélection interactive"""
        import asyncio
        
        try:
            # Récupérer les données de la série
            instance = self._get_sonarr_instance_by_id(instance_id) if instance_id else self._get_sonarr_instance(show_id)
            if not instance:
                raise Exception("Aucune instance Sonarr trouvée pour cette série")

            client = SonarrClient(instance.url, instance.api_key, instance.id)
            series_data = await self._get_series_data(client, show_id)
            show_title = series_data.get("title", "Série inconnue")
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)

            # Vérifier si l’opération a été annulée
            if asyncio.current_task().cancelled():
                logger.info(f"🚫 Recherche annulée pour {show_title} Saison {season_number}")
                # Envoyer une notification d’annulation
                await manager.send_personal_message({
                    "type": "clear_progress",
                    "operation_type": "interactive_search",
                    "message": "🚫 Opération de recherche annulée par l’utilisateur"
                }, self.user_id)
                raise asyncio.CancelledError()

            # Notifier le démarrage de la recherche
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "interactive_search",
                f"🔍 Recherche des Releases pour la saison {season_number}...",
                20,
                current_step="Recherche",
                details={"poster_url": poster_url, "season_number": season_number}
            )

            # Rechercher les Releases (opération lente)
            releases = await client._get_releases(show_id, season_number)

            if not all(isinstance(r, dict) for r in releases):
                logger.error(f"❌ Données inattendues dans releases: {[type(r).__name__ for r in releases]}")
                releases = [r for r in releases if isinstance(r, dict)]

            releases = self._filter_approved_releases(releases)
            logger.debug(f"Types après filtre : {[type(r).__name__ for r in releases]}")
            # Vérifier si annulation après appel API
            if asyncio.current_task().cancelled():
                logger.info(f"🚫 Recherche annulée pour {show_title} Saison {season_number} après appel API")
                await manager.send_personal_message({
                    "type": "clear_progress",
                    "operation_type": "interactive_search",
                    "message": "🚫 Opération de recherche annulée par l’utilisateur"
                }, self.user_id)
                raise asyncio.CancelledError()
            
            # Notifier les résultats trouvés
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "interactive_search",
                f"📊 {len(releases)} packs de saison trouvés",
                70,
                current_step="Analyse des résultats",
                details={"poster_url": poster_url, "season_number": season_number, "releases_found": len(releases)}
            )

            # Formater les résultats pour l’interface
            formatted_releases = []
            logger.info(f"Formatage de {len(releases)} résultats pour l’UI")
            for i, release in enumerate(releases):

                if not isinstance(release, dict):
                    logger.error(f"❌ Release inattendu à l'index {i}: type={type(release).__name__}, valeur={release}")
                    continue  # on saute cet élément

                if i == 0:  # Log du premier résultat pour analyse
                    logger.info(f"Clés disponibles du premier résultat : {list(release.keys())}")
                    logger.info(f"Score de format personnalisé du premier résultat : {release.get('customFormatScore', 'NOT_FOUND')}")
                    logger.info(f"IndexerId du premier résultat : {release.get('indexerId', 'NOT_FOUND')}")
                try:
                    formatted_release = self._format_release_for_ui(release)
                except Exception:
                    logger.exception(f"💥 Crash pendant _format_release_for_ui à l'index {i} (type={type(release).__name__})")
                    continue


                logger.info(
                    f"Résultat formaté {i+1} : "
                    f"quality_score={formatted_release.get('quality_score', 'NOT_FOUND')}, "
                    f"indexer_id={formatted_release.get('indexer_id', 'NOT_FOUND')}"
                )
                 
                formatted_releases.append(formatted_release)

            # Trier par qualité et seeders
            formatted_releases.sort(key=lambda x: (-x["release_weight"], -x["seeders"]))

            # Notifier fin de recherche
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "interactive_search",
                f"✅ Recherche interactive terminée - {len(formatted_releases)} résultats prêts à être sélectionnés",
                100,
                "success",
                current_step="Terminé",
                details={"poster_url": poster_url, "season_number": season_number, "releases_found": len(formatted_releases)}
            )

            return formatted_releases

        except Exception as e:
            logger.error(f"Erreur dans la recherche interactive : {e}")
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title if 'show_title' in locals() else "Série inconnue",
                "interactive_search",
                f"❌ Échec de la recherche : {str(e)}",
                100,
                "error",
                current_step="Erreur",
                details={"error": str(e)}
            )
            raise

    async def download_specific_release(self, release_guid: str, show_id: int, season_number: int, instance_id: Optional[int] = None, indexer_id: Optional[int] = None) -> Dict[str, Any]:
        """Télécharger une sortie spécifique via son GUID"""
        activity = None
        try:
            # Récupérer les données de la série
            instance = self._get_sonarr_instance_by_id(instance_id) if instance_id else self._get_sonarr_instance(show_id)
            if not instance:
                raise Exception("Aucune instance Sonarr trouvée pour cette série")

            client = SonarrClient(instance.url, instance.api_key, instance.id)
            series_data = await self._get_series_data(client, show_id)
            show_title = series_data.get("title", "Série inconnue")
            poster_url = client._get_banner_url(series_data.get("images", []), client.instance_id)

            # Créer un log d’activité
            activity = self._create_activity_log(instance.id, show_id, show_title, season_number)

            # Notifier le début du téléchargement
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "manual_download",
                f"🚀 Lancement du téléchargement manuel pour la saison {season_number}...",
                10,
                current_step="Initialisation",
                details={"poster_url": poster_url, "season_number": season_number}
            )

            # Vérifier les paramètres utilisateur
            settings = self.db.query(UserSettings).filter(UserSettings.user_id == self.user_id).first()
            skip_deletion = settings and settings.skip_episode_deletion

            if not skip_deletion:
                # Supprimer les épisodes existants
                await manager.send_enhanced_progress_update(
                    self.user_id,
                    show_title,
                    "manual_download",
                    f"🗑️ Suppression des épisodes existants de la saison {season_number}...",
                    30,
                    current_step="Suppression des épisodes",
                    details={"poster_url": poster_url, "season_number": season_number}
                )
                
                await client.delete_season_episodes(show_id, season_number)

            # Télécharger la sortie spécifique
            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title,
                "manual_download",
                f"📥 Téléchargement de la sortie sélectionnée...",
                60,
                current_step="Téléchargement",
                details={"poster_url": poster_url, "season_number": season_number}
            )

            # Utiliser l’API de téléchargement direct de Sonarr
            download_result = await client.download_release_direct(release_guid, indexer_id)

            # Notification de fin
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

            # Mettre à jour le log d’activité
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
            logger.error(f"Erreur lors du téléchargement de la sortie : {e}")
            
            # Mise à jour du log en cas d’erreur
            if activity:
                self._update_activity_log(
                    activity,
                    "error",
                    f"Échec du téléchargement manuel pour {activity.show_title}",
                    str(e)
                )

            await manager.send_enhanced_progress_update(
                self.user_id,
                show_title if 'show_title' in locals() else "Série inconnue",
                "manual_download",
                f"❌ Échec du téléchargement : {str(e)}",
                100,
                "error",
                current_step="Erreur",
                details={"error": str(e)}
            )
            raise

    def _format_release_for_ui(self, release: Dict[str, Any]) -> Dict[str, Any]:
        """Formater une sortie pour l’affichage dans l’interface"""
        # Fonction utilitaire pour formater la taille
        def format_size(size_bytes):
            if size_bytes >= 1024**3:
                return f"{size_bytes / (1024**3):.1f} Go"
            elif size_bytes >= 1024**2:
                return f"{size_bytes / (1024**2):.1f} Mo"
            elif size_bytes >= 1024:
                return f"{size_bytes / 1024:.1f} Ko"
            else:
                return f"{size_bytes} o"

        # Fonction utilitaire pour formater l’âge
        def format_age(age_hours):
            if age_hours < 1:
                return "< 1 heure"
            elif age_hours < 24:
                return f"{int(age_hours)} heures"
            elif age_hours < 24 * 7:
                return f"{int(age_hours / 24)} jours"
            elif age_hours < 24 * 30:
                return f"{int(age_hours / (24 * 7))} semaines"
            elif age_hours < 24 * 365:
                return f"{int(age_hours / (24 * 30))} mois"
            else:
                # Conversion en années et mois
                total_months = int(age_hours / (24 * 30))
                years = total_months // 12
                months = total_months % 12
                if months == 0:
                    return f"{years} ans"
                else:
                    return f"{years} ans {months} mois"

        # Extraction des infos de qualité
        quality = release.get("quality", {})
        
        if "quality" in quality:
            quality_info = quality.get("quality", {})
            quality_name = quality_info.get("name", "Inconnue")
        else:
            quality_name = quality.get("name", "Inconnue")
        
        # Score de qualité basé sur le customFormatScore de l’API Sonarr
        quality_score = release.get("customFormatScore", 0)
        logger.info(f"Release: {release.get('title', 'Inconnue')[:50]}... - customFormatScore: {quality_score}")
        
        # Calcul du poids de la release pour le tri
        release_weight = quality_score
        
        if release.get("proper", False):
            release_weight += 50
        if "repack" in release.get("title", "").lower():
            release_weight += 25

        return {
            "guid": release.get("guid", ""),
            "title": release.get("title", "Inconnue"),
            "size": release.get("size", 0),
            "size_formatted": format_size(release.get("size", 0)),
            "seeders": release.get("seeders", 0),
            "leechers": release.get("leechers", 0),
            "age": release.get("age", 0),
            "age_formatted": format_age(release.get("ageHours", 0)),
            "quality": quality_name,
            "quality_score": quality_score,
            "indexer": release.get("indexer", "Inconnu"),
            "indexer_id": release.get("indexerId", 0),
            "approved": release.get("approved", True),
            "indexer_flags": release.get("indexerFlags", []),
            "rejections": release.get("rejections", []),
            "release_weight": release_weight
        }

