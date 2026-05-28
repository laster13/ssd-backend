import asyncio
import logging
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from program.settings.manager import config_manager
from integrations.seasonarr.db.models import SonarrInstance
from integrations.seasonarr.clients.sonarr_client import SonarrClient
from integrations.seasonarr.services.season_it_service import SeasonItService

logger = logging.getLogger(__name__)


class AutoSeasonarrMissingService:
    def __init__(self, db: Session):
        self.db = db

    def _get_value(self, item: Any, *keys: str, default: Any = None) -> Any:
        """
        Lit une valeur depuis un dict ou un objet Pydantic/classique.
        """
        for key in keys:
            if isinstance(item, dict):
                value = item.get(key)
            else:
                value = getattr(item, key, None)

            if value is not None:
                return value

        return default

    def _get_shows_from_response(self, response: Any) -> List[Any]:
        """
        Compatible avec :
        - dict {"shows": [...]}
        - objet ShowResponse avec .shows
        - liste directe
        """
        if isinstance(response, dict):
            return response.get("shows", []) or []

        if isinstance(response, list):
            return response

        return getattr(response, "shows", []) or []

    def _get_missing_episode_count(self, show: Any) -> int:
        """
        Récupère le nombre d'épisodes manquants depuis un dict ou un objet.
        """
        value = self._get_value(
            show,
            "missing_episode_count",
            "missingEpisodeCount",
            "missingEpisodes",
            default=0,
        )

        try:
            return int(value or 0)
        except Exception:
            return 0

    async def run_once(self, max_shows_per_run: int = 50) -> Dict[str, Any]:
        """
        Lance SeasonIt automatiquement sur les séries Sonarr avec épisodes manquants.

        Important :
        - ne lance pas de scan Sonarr global ;
        - utilise la liste déjà connue via Sonarr/Seasonarr ;
        - SeasonIt garde la sécurité pack Sonarr + cache AllDebrid.
        """
        enabled = bool(
            getattr(config_manager.config, "auto_seasonarr_missing_enabled", False)
        )

        if not enabled:
            logger.info(
                "Auto Seasonarr missing ignoré : auto_seasonarr_missing_enabled=False"
            )

            return {
                "status": "disabled",
                "message": "Auto Seasonarr missing désactivé",
                "processed": 0,
                "results": [],
            }

        instances = (
            self.db.query(SonarrInstance)
            .filter(SonarrInstance.is_active == True)
            .all()
        )

        if not instances:
            logger.warning("Auto Seasonarr missing : aucune instance Sonarr active")

            return {
                "status": "no_instances",
                "message": "Aucune instance Sonarr active",
                "processed": 0,
                "results": [],
            }

        results: List[Dict[str, Any]] = []
        processed = 0

        for instance in instances:
            if processed >= max_shows_per_run:
                break

            logger.info(
                "Auto Seasonarr missing : analyse instance Sonarr id=%s user=%s",
                instance.id,
                instance.owner_id,
            )

            client = SonarrClient(instance.url, instance.api_key, instance.id)

            try:
                response = await client.get_series(
                    page=1,
                    page_size=10000,
                    missing_episodes=True,
                )
            except Exception as e:
                logger.error(
                    "Auto Seasonarr missing : erreur récupération séries instance=%s : %s",
                    instance.id,
                    e,
                    exc_info=True,
                )

                results.append(
                    {
                        "instance_id": instance.id,
                        "status": "error_get_series",
                        "error": str(e),
                    }
                )

                continue

            shows = self._get_shows_from_response(response)

            missing_shows = [
                show
                for show in shows
                if self._get_missing_episode_count(show) > 0
            ]

            logger.info(
                "Auto Seasonarr missing : %s série(s) avec épisodes manquants trouvée(s) sur instance=%s",
                len(missing_shows),
                instance.id,
            )

            service = SeasonItService(self.db, instance.owner_id)

            for show in missing_shows:
                if processed >= max_shows_per_run:
                    break

                show_id = self._get_value(show, "id")
                show_title = self._get_value(show, "title", default=f"Show {show_id}")

                if not show_id:
                    logger.debug(
                        "Auto Seasonarr missing : série ignorée car id absent"
                    )
                    continue

                try:
                    logger.info(
                        "Auto Seasonarr missing : lancement SeasonIt pour %s",
                        show_title,
                    )

                    result = await service.process_season_it(
                        show_id=int(show_id),
                        season_number=None,
                        instance_id=instance.id,
                    )

                    results.append(
                        {
                            "instance_id": instance.id,
                            "show_id": show_id,
                            "show": show_title,
                            "status": "processed",
                            "result": result,
                        }
                    )

                    processed += 1

                    await asyncio.sleep(3)

                except Exception as e:
                    logger.error(
                        "Auto Seasonarr missing : échec SeasonIt pour %s : %s",
                        show_title,
                        e,
                        exc_info=True,
                    )

                    results.append(
                        {
                            "instance_id": instance.id,
                            "show_id": show_id,
                            "show": show_title,
                            "status": "error",
                            "error": str(e),
                        }
                    )

        return {
            "status": "completed",
            "processed": processed,
            "results": results,
        }