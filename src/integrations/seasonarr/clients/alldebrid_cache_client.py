import logging
import os
import re
from typing import Any, List, Optional, Set

import httpx

logger = logging.getLogger(__name__)
alldebrid_logger = logging.getLogger("ALLDEBRID")


class AllDebridCacheClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.alldebrid.com/v4.1"

        # Proxy optionnel.
        # Si ALLDEBRID_PROXY est vide ou absent, le comportement reste identique à avant.
        # Exemple Oracle/WARP :
        # ALLDEBRID_PROXY=socks5h://sfr-warp:1080
        self.proxy = os.getenv("ALLDEBRID_PROXY") or None

    def extract_info_hash(self, value: Any) -> Optional[str]:
        """
        Extrait un infoHash depuis :
        - un hash brut,
        - un magnet,
        - une URL contenant btih,
        - un dict contenant hash/magnet/infoHash.
        """
        if not value:
            return None

        if isinstance(value, dict):
            value = (
                value.get("hash")
                or value.get("magnet")
                or value.get("magnetUrl")
                or value.get("magnet_url")
                or value.get("infoHash")
                or value.get("info_hash")
            )

        if not value:
            return None

        value = str(value).strip()

        if re.fullmatch(r"[a-fA-F0-9]{40}", value):
            return value.lower()

        match = re.search(r"btih:([a-fA-F0-9]{40})", value)
        if match:
            return match.group(1).lower()

        return None

    async def _delete_magnet(
        self,
        client: httpx.AsyncClient,
        magnet_id: Optional[int],
    ) -> None:
        """
        Supprime le magnet ajouté pendant la vérification.
        On ne bloque jamais le flux si la suppression échoue.
        """
        if not magnet_id:
            return

        try:
            response = await client.post(
                f"{self.base_url}/magnet/delete",
                headers={"Authorization": f"Bearer {self.api_key}"},
                data={"id": magnet_id},
            )

        except Exception as e:
            logger.debug(
                "AllDebrid - Erreur pendant la suppression du magnet de vérification id=%s : %s",
                magnet_id,
                e,
                exc_info=True,
            )

    async def get_cached_hashes(self, hashes_or_magnets: List[Any]) -> Set[str]:
        """
        Vérifie le cache AllDebrid avec /v4.1/magnet/upload.

        Accepte :
        - un hash brut,
        - un magnet,
        - une URL contenant btih,
        - un dict {"hash": "...", "name": "..."} pour garder le nom de release.
        """
        cached_hashes: Set[str] = set()
        normalized_values = []

        for value in hashes_or_magnets:
            display_name = None

            if isinstance(value, dict):
                display_name = (
                    value.get("name")
                    or value.get("title")
                    or value.get("releaseTitle")
                )

            info_hash = self.extract_info_hash(value)

            if info_hash:
                normalized_values.append(
                    {
                        "hash": info_hash,
                        "name": display_name or info_hash,
                    }
                )
            else:
                logger.debug(
                    "AllDebrid - Valeur ignorée : aucun infoHash exploitable"
                )

        if not normalized_values:
            logger.warning(
                "AllDebrid - Vérification cache impossible : aucun hash exploitable"
            )
            return cached_hashes

        unique_values_by_hash = {}

        for item in normalized_values:
            unique_values_by_hash[item["hash"]] = item

        unique_values = list(unique_values_by_hash.values())

        logger.info(
            "AllDebrid - Vérification cache : %s hash(es)",
            len(unique_values),
        )

        endpoint = f"{self.base_url}/magnet/upload"

        client_kwargs = {
            "timeout": 30.0,
        }

        if self.proxy:
            client_kwargs["proxy"] = self.proxy
            logger.info(
                "AllDebrid - Proxy actif pour vérification cache : %s",
                self.proxy,
            )
        else:
            logger.info(
                "AllDebrid - Aucun proxy actif pour vérification cache"
            )

        try:
            async with httpx.AsyncClient(**client_kwargs) as client:
                for item in unique_values:
                    info_hash = item["hash"]
                    display_name = item["name"]

                    try:
                        response = await client.post(
                            endpoint,
                            headers={"Authorization": f"Bearer {self.api_key}"},
                            data={"magnets[]": info_hash},
                        )
                    except Exception as e:
                        logger.warning(
                            "AllDebrid - Erreur réseau pendant la vérification cache : hash=%s erreur=%s",
                            info_hash,
                            e,
                        )
                        continue

                    if response.status_code != 200:
                        logger.warning(
                            "AllDebrid - Vérification cache échouée : hash=%s status=%s",
                            info_hash,
                            response.status_code,
                        )
                        continue

                    try:
                        payload = response.json()
                    except Exception as e:
                        logger.warning(
                            "AllDebrid - Réponse invalide pendant la vérification cache : hash=%s erreur=%s",
                            info_hash,
                            e,
                        )
                        continue

                    if payload.get("status") != "success":
                        logger.warning(
                            "AllDebrid - Réponse non valide pendant la vérification cache : hash=%s status=%s",
                            info_hash,
                            payload.get("status"),
                        )
                        continue

                    data = payload.get("data", {})
                    magnets = data.get("magnets", [])

                    if not isinstance(magnets, list) or not magnets:
                        logger.debug(
                            "AllDebrid - Aucun résultat exploitable pour le hash : %s",
                            info_hash,
                        )
                        continue

                    for magnet in magnets:
                        sent_value = str(magnet.get("magnet", "")).strip()
                        returned_hash = self.extract_info_hash(
                            magnet.get("hash") or sent_value
                        )

                        magnet_id = magnet.get("id")
                        ready = bool(magnet.get("ready", False))
                        error = magnet.get("error")

                        name = (
                            display_name
                            or magnet.get("name")
                            or returned_hash
                            or info_hash
                        )

                        if error:
                            logger.debug(
                                "AllDebrid - Hash rejeté pendant la vérification cache : %s erreur=%s",
                                returned_hash or info_hash,
                                error,
                            )
                            await self._delete_magnet(client, magnet_id)
                            continue

                        if ready and returned_hash:
                            alldebrid_logger.log(
                                25,
                                "Cache confirmé : %s",
                                name,
                            )
                            cached_hashes.add(returned_hash)
                        else:
                            alldebrid_logger.log(
                                25,
                                "Cache non confirmé : %s",
                                name,
                            )

                        await self._delete_magnet(client, magnet_id)

        except Exception as e:
            logger.warning(
                "AllDebrid - Erreur pendant la vérification cache : %s",
                e,
                exc_info=True,
            )
            return set()

        logger.info(
            "AllDebrid - Résultat cache : %s/%s confirmé(s)",
            len(cached_hashes),
            len(unique_values),
        )

        return cached_hashes