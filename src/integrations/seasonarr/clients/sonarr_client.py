import httpx
from typing import List, Dict, Optional, Any
import logging
from ..db.schemas import ShowResponse

logger = logging.getLogger(__name__)

class SonarrClient:
    def __init__(self, base_url: str, api_key: str, instance_id: int = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.instance_id = instance_id
        self.headers = {
            "X-Api-Key": api_key,
            "Content-Type": "application/json"
        }

    async def test_connection(self) -> bool:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/api/v3/system/status",
                    headers=self.headers,
                    timeout=10.0
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    async def get_series(self, page: int = 1, page_size: int = 35, search: str = "", status: str = "", monitored: bool = None, missing_episodes: bool = None, network: str = "", genres: List[str] = None, year_from: int = None, year_to: int = None, runtime_min: int = None, runtime_max: int = None, certification: str = "") -> Dict[str, Any]:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/api/v3/series",
                    headers=self.headers,
                    timeout=30.0
                )
                
                if response.status_code != 200:
                    raise Exception(f"Sonarr API error: {response.status_code}")
                
                series_data = response.json()
                
                shows = []
                for series in series_data:
                    series_title = series["title"]
                    series_status = series.get("status", "unknown")
                    series_monitored = series.get("monitored", False)
                    series_stats = series.get("statistics", {})
                    episode_count = series_stats.get("episodeCount", 0)
                    episode_file_count = series_stats.get("episodeFileCount", 0)
                    missing_count = episode_count - episode_file_count
                    
                    # Extract additional fields for filtering
                    series_network = series.get("network", "")
                    series_genres = series.get("genres", [])
                    series_year = series.get("year")
                    series_runtime = series.get("runtime")
                    series_certification = series.get("certification", "")
                    
                    # Apply search filter
                    if search and search.lower() not in series_title.lower():
                        continue
                    
                    # Apply status filter
                    if status and series_status.lower() != status.lower():
                        continue
                    
                    # Apply monitored filter
                    if monitored is not None and series_monitored != monitored:
                        continue
                    
                    # Apply missing episodes filter
                    if missing_episodes is not None:
                        if missing_episodes and missing_count <= 0:
                            continue
                        if not missing_episodes and missing_count > 0:
                            continue
                    
                    # Apply network filter
                    if network and series_network.lower() != network.lower():
                        continue
                    
                    # Apply genre filter
                    if genres and len(genres) > 0:
                        if not any(genre.lower() in [g.lower() for g in series_genres] for genre in genres):
                            continue
                    
                    # Apply year range filter
                    if year_from is not None and (series_year is None or series_year < year_from):
                        continue
                    if year_to is not None and (series_year is None or series_year > year_to):
                        continue
                    
                    # Apply runtime filter
                    if runtime_min is not None and (series_runtime is None or series_runtime < runtime_min):
                        continue
                    if runtime_max is not None and (series_runtime is None or series_runtime > runtime_max):
                        continue
                    
                    # Apply certification filter
                    if certification and series_certification.lower() != certification.lower():
                        continue
                    
                    # Process seasons with statistics
                    processed_seasons = []
                    for season in series.get("seasons", []):
                        season_stats = season.get("statistics", {})
                        if season_stats:
                            season_episode_count = season_stats.get("episodeCount", 0)
                            season_episode_file_count = season_stats.get("episodeFileCount", 0)
                            season_missing_count = season_episode_count - season_episode_file_count
                        else:
                            season_episode_count = 0
                            season_episode_file_count = 0
                            season_missing_count = 0
                        
                        processed_seasons.append({
                            "seasonNumber": season.get("seasonNumber"),
                            "monitored": season.get("monitored", False),
                            "episodeCount": season_episode_count,
                            "episodeFileCount": season_episode_file_count,
                            "missing_episode_count": season_missing_count,
                            "totalEpisodeCount": season_stats.get("totalEpisodeCount", 0) if season_stats else 0,
                            "percentOfEpisodes": season_stats.get("percentOfEpisodes", 0.0) if season_stats else 0.0
                        })

                    overview = series.get("overview", "")
                    
                    show = ShowResponse(
                        id=series["id"],
                        title=series_title,
                        year=series_year,
                        poster_url=self._get_poster_url(series.get("images", []), self.instance_id),
                        status=series_status,
                        monitored=series_monitored,
                        episode_count=episode_count,
                        missing_episode_count=missing_count,
                        seasons=processed_seasons,
                        network=series_network,
                        genres=series_genres,
                        runtime=series_runtime,
                        certification=series_certification,
                        overview=overview
                    )
                    shows.append(show)
                
                # Note: hide_incomplete_seasons functionality removed as it's redundant
                # Season It operations already skip incomplete seasons automatically
                
                start = (page - 1) * page_size
                end = start + page_size
                paginated_shows = shows[start:end]
                
                return {
                    "shows": paginated_shows,
                    "total": len(shows),
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (len(shows) + page_size - 1) // page_size
                }
                
        except Exception as e:
            logger.error(f"Error fetching series: {e}")
            raise

    async def get_show_detail(self, show_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific show including episodes"""
        try:
            async with httpx.AsyncClient() as client:
                # Get series details
                series_response = await client.get(
                    f"{self.base_url}/api/v3/series/{show_id}",
                    headers=self.headers,
                    timeout=30.0
                )
                if series_response.status_code != 200:
                    raise Exception(f"Sonarr API error: {series_response.status_code}")
                series_data = series_response.json()

                # Get episodes for this series
                episodes_response = await client.get(
                    f"{self.base_url}/api/v3/episode",
                    params={"seriesId": show_id},
                    headers=self.headers,
                    timeout=30.0
                )
                episodes_data = []
                if episodes_response.status_code == 200:
                    episodes_data = episodes_response.json()

                # Group episodes by season
                seasons_with_episodes = {}
                for episode in episodes_data:
                    season_num = episode.get("seasonNumber", 0)
                    if season_num not in seasons_with_episodes:
                        seasons_with_episodes[season_num] = []
                    seasons_with_episodes[season_num].append(episode)

                # Check for incomplete seasons
                future_check = await self.has_future_episodes(show_id)
                incomplete_seasons = set(future_check.get("seasons_incomplete", []))

                # Process seasons with detailed statistics
                processed_seasons = []
                for season in series_data.get("seasons", []):
                    season_num = season.get("seasonNumber", 0)
                    season_stats = season.get("statistics", {})
                    season_episodes = seasons_with_episodes.get(season_num, [])

                    total_episodes = len(season_episodes)
                    downloaded_episodes = len([ep for ep in season_episodes if ep.get("hasFile", False)])
                    missing_episodes = total_episodes - downloaded_episodes

                    processed_seasons.append({
                        "seasonNumber": season_num,
                        "monitored": season.get("monitored", False),
                        "episodeCount": total_episodes,
                        "episodeFileCount": downloaded_episodes,
                        "missing_episode_count": missing_episodes,
                        "totalEpisodeCount": season_stats.get("totalEpisodeCount", total_episodes),
                        "percentOfEpisodes": season_stats.get("percentOfEpisodes", 0.0),
                        "episodes": season_episodes,
                        "has_future_episodes": season_num in incomplete_seasons
                    })

                # Calculate overall statistics
                series_stats = series_data.get("statistics", {})
                total_episode_count = series_stats.get("episodeCount", 0)
                episode_file_count = series_stats.get("episodeFileCount", 0)
                missing_count = total_episode_count - episode_file_count

                return {
                    "id": series_data["id"],
                    "title": series_data["title"],
                    "year": series_data.get("year"),
                    "overview": series_data.get("overview", ""),
                    "network": series_data.get("network", ""),
                    "status": series_data.get("status", "unknown"),
                    "monitored": series_data.get("monitored", False),
                    "poster_url": self._get_poster_url(series_data.get("images", []), self.instance_id),
                    "banner_url": self._get_banner_url(series_data.get("images", []), self.instance_id),
                    "episode_count": total_episode_count,
                    "missing_episode_count": missing_count,
                    "seasons": processed_seasons,
                    "path": series_data.get("path", ""),   # 👈 champ ajouté
                    "qualityProfileId": series_data.get("qualityProfileId"),
                    "languageProfileId": series_data.get("languageProfileId"),
                    "seriesType": series_data.get("seriesType", "standard"),
                    "seasonFolder": series_data.get("seasonFolder", True),
                    "useSceneNumbering": series_data.get("useSceneNumbering", False),
                    "runtime": series_data.get("runtime", 0),
                    "tvdbId": series_data.get("tvdbId"),
                    "tvMazeId": series_data.get("tvMazeId"),
                    "imdbId": series_data.get("imdbId"),
                    "titleSlug": series_data.get("titleSlug", ""),
                    "certification": series_data.get("certification", ""),
                    "genres": series_data.get("genres", []),
                    "tags": series_data.get("tags", []),
                    "added": series_data.get("added", ""),
                    "ratings": series_data.get("ratings", {}),
                    "ended": series_data.get("ended", False)
                }

        except Exception as e:
            logger.error(f"Erreur lors de la récupération des détails de la série : {e}")
            raise

    def _get_banner_url(self, images: List[Dict], instance_id: int = None) -> Optional[str]:
        for image in images:
            if image.get("coverType") == "banner":
                if instance_id:
                    # URL encode the image path to handle query parameters
                    from urllib.parse import quote
                    encoded_url = quote(image['url'], safe='')
                    return f"/api/v1/proxy-image?url={encoded_url}&instance_id={instance_id}"
                else:
                    # Fallback to direct URL (may not work from frontend)
                    return f"{self.base_url}{image['url']}"
        return None

    def _get_poster_url(self, images: List[Dict], instance_id: int = None) -> Optional[str]:
        for image in images:
            if image.get("coverType") == "poster":
                if instance_id:
                    # URL encode the image path to handle query parameters
                    from urllib.parse import quote
                    encoded_url = quote(image['url'], safe='')
                    return f"/api/v1/proxy-image?url={encoded_url}&instance_id={instance_id}"
                else:
                    # Fallback to direct URL (may not work from frontend)
                    return f"{self.base_url}{image['url']}"
        return None

    async def search_season_pack(self, series_id: int, season_number: int):
        """Trigger a season search command for Sonarr to find and download season packs"""
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                search_data = {
                    "name": "SeasonSearch",
                    "seriesId": series_id,
                    "seasonNumber": season_number
                }
                
                logger.info(f"Triggering SeasonSearch command for series {series_id} season {season_number}")
                response = await client.post(
                    f"{self.base_url}/api/v3/command",
                    headers=self.headers,
                    json=search_data
                )
                
                if response.status_code != 201:
                    logger.error(f"Échec de la commande SeasonSearch :: {response.status_code} - {response.text}")
                    raise Exception(f"SeasonSearch command failed: {response.status_code}")
                
                command_id = response.json()["id"]
                logger.info(f"Commande SeasonSearch {command_id} soumise avec succès")
                
                # Wait for the command to complete
                await self._wait_for_command(command_id)
                logger.info(f"Commande SeasonSearch {command_id} complète")
                
                return command_id
                
        except Exception as e:
            logger.error(f"Erreur lors du déclenchement de la recherche de saison : {e}")
            raise

    async def monitor_download_progress(self, series_id: int, season_number: int, max_wait: int = 300, user_id: int = None) -> Dict[str, Any]:
        """Monitor for successful downloads after a season search"""
        import asyncio
        from websocket_manager import manager
        from datetime import datetime, timedelta
        
        try:
            # Get baseline time for checking recent activity
            start_time = datetime.now().astimezone()
            logger.info(f"Démarrage de la surveillance des téléchargements pour la série {series_id} saison {season_number} at {start_time}")
            
            # Wait a bit for the search to find releases
            await asyncio.sleep(3)
            
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                checks = 0
                max_checks = max_wait // 5
                
                for _ in range(max_checks):  # Check every 5 seconds
                    checks += 1
                    
                    # Send periodic updates
                    if user_id and checks % 6 == 0:  # Every 30 seconds
                        await manager.send_progress_update(
                            user_id, 
                            f"Still monitoring for downloads... ({checks * 5}s elapsed)", 
                            85
                        )
                    
                    # Check history for grabbed releases first (most reliable)
                    try:
                        history_response = await client.get(
                            f"{self.base_url}/api/v3/history",
                            params={
                                "seriesId": series_id, 
                                "eventType": "grabbed",
                                "pageSize": 50
                            },
                            headers=self.headers
                        )
                        
                        if history_response.status_code == 200:
                            history_data = history_response.json()
                            records = history_data.get("records", [])
                            logger.info(f"Trouvé {len(records)} Enregistrements récupérés pour la série {series_id}")
                            
                            # Look for recent grabs for our series/season
                            for record in records:
                                episode = record.get("episode", {})
                                data = record.get("data", {})
                                
                                # Check if this record is for our season
                                if episode.get("seasonNumber") == season_number:
                                    date_str = record.get("date", "")
                                    if date_str:
                                        try:
                                            # Parse the date and check if it's recent (since we started)
                                            record_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                            
                                            # Check if this grab happened after we started monitoring
                                            if record_date > start_time - timedelta(minutes=1):
                                                title = record.get("sourceTitle", "Unknown Release")
                                                
                                                # Check if it's a season pack
                                                is_season_pack = any([
                                                    "season" in title.lower(),
                                                    "complete" in title.lower(),
                                                    data.get("fullSeason", False)
                                                ])
                                                
                                                logger.info(f"Récupération récente trouvée : {title}, isSeasonPack: {is_season_pack}, date: {record_date}")
                                                
                                                return {
                                                    "status": "grabbed",
                                                    "title": title,
                                                    "message": f"Récupéré avec succès : {title}",
                                                    "is_season_pack": is_season_pack
                                                }
                                        except Exception as date_error:
                                            logger.error(f"Erreur lors de l'analyse de la date {date_str}: {date_error}")
                                            continue
                    except Exception as history_error:
                        logger.error(f"Erreur lors de la vérification de l’historique : {history_error}")
                    
                    # Check queue for active downloads
                    try:
                        queue_response = await client.get(
                            f"{self.base_url}/api/v3/queue",
                            headers=self.headers
                        )
                        
                        if queue_response.status_code == 200:
                            queue_data = queue_response.json()
                            records = queue_data.get("records", [])
                            
                            # Look for downloads for our series/season
                            for record in records:
                                episode = record.get("episode", {})
                                if (record.get("seriesId") == series_id and 
                                    episode.get("seasonNumber") == season_number):
                                    
                                    status = record.get("status", "")
                                    title = record.get("title", "Unknown Release")
                                    
                                    logger.info(f"Élément trouvé dans la file d’attente : {title}, status: {status}")
                                    
                                    if status in ["downloading", "queued"]:
                                        return {
                                            "status": "downloading",
                                            "title": title,
                                            "message": f"Téléchargement: {title}"
                                        }
                                    elif status == "completed":
                                        return {
                                            "status": "completed",
                                            "title": title,
                                            "message": f"Téléchargé avec succès : {title}"
                                        }
                    except Exception as queue_error:
                        logger.error(f"Erreur lors de la vérification de la file d’attente : {queue_error}")
                    
                    await asyncio.sleep(5)
                
                # Timeout reached - check one more time for any grabs
                logger.info(f"Délai d’attente de surveillance atteint après {max_wait}s")
                
                # Final check of history
                try:
                    history_response = await client.get(
                        f"{self.base_url}/api/v3/history",
                        params={
                            "seriesId": series_id, 
                            "eventType": "grabbed",
                            "pageSize": 10
                        },
                        headers=self.headers
                    )
                    
                    if history_response.status_code == 200:
                        history_data = history_response.json()
                        records = history_data.get("records", [])
                        
                        for record in records:
                            episode = record.get("episode", {})
                            if episode.get("seasonNumber") == season_number:
                                date_str = record.get("date", "")
                                if date_str:
                                    try:
                                        record_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                        if record_date > start_time - timedelta(minutes=1):
                                            title = record.get("sourceTitle", "Unknown Release")
                                            return {
                                                "status": "grabbed",
                                                "title": title,
                                                "message": f"Successfully grabbed: {title}"
                                            }
                                    except:
                                        continue
                except:
                    pass
                
                return {
                    "status": "timeout",
                    "message": "Recherche terminée mais impossible de confirmer l’état du téléchargement"
                }
                
        except Exception as e:
            logger.error(f"Erreur lors de la surveillance de la progression du téléchargement : {e}")
            return {
                "status": "error",
                "message": f"Erreur lors de la surveillance du téléchargement : {str(e)}"
            }

    async def _wait_for_command(self, command_id: int, max_wait: int = 60):
        import asyncio
        
        async with httpx.AsyncClient() as client:
            for _ in range(max_wait):
                response = await client.get(
                    f"{self.base_url}/api/v3/command/{command_id}",
                    headers=self.headers
                )
                
                if response.status_code == 200:
                    status = response.json().get("status")
                    if status in ["completed", "failed"]:
                        break
                
                await asyncio.sleep(1)

    async def _get_releases(self, series_id: int, season_number: int) -> List[Dict]:
        import asyncio
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                # Check for cancellation before making the API call
                if asyncio.current_task().cancelled():
                    logger.info(f"🚫 Récupération des Releases annulée pour la série {series_id} saison {season_number}")
                    raise asyncio.CancelledError()
                
                async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
                    logger.info(f"Récupération des Releases pour la série {series_id} saison {season_number} (attempt {attempt + 1})")
                    response = await client.get(
                        f"{self.base_url}/api/v3/release",
                        params={"seriesId": series_id, "seasonNumber": season_number},
                        headers=self.headers
                    )
                
                # Check for cancellation after the API call
                if asyncio.current_task().cancelled():
                    logger.info(f"🚫 Récupération des Releases annulée pour la série {series_id} saison {season_number} after API call")
                    raise asyncio.CancelledError()
                
                logger.info(f"Statut de la réponse de l’API des Releases : {response.status_code}")
                
                if response.status_code == 200:
                    releases = response.json()
                    logger.info(f"Trouvé {len(releases)} releases")
                    
                    # Filter for season packs using the fullSeason field
                    season_packs = [r for r in releases if r.get("fullSeason", False)]
                    logger.info(f"Trouvé {len(season_packs)} Packs de saison avant filtrage multi-saisons")
                    
                    # Filter out multi-season packs
                    single_season_packs = []
                    for release in season_packs:
                        title = release.get("title", "").lower()
                        
                        # Check for multi-season indicators in the title
                        multi_season_indicators = [
                            f"s{season_number:02d}-s",  # S01-S02, S01-S03, etc.
                            f"s{season_number}-s",      # S1-S2, S1-S3, etc.
                            f"season {season_number}-",  # Season 1-2, Season 1-3, etc.
                            f"seasons {season_number}-", # Seasons 1-2, Seasons 1-3, etc.
                            "complete series",
                            "all seasons",
                            "seasons 1-",
                            "s01-s",
                            "s1-s"
                        ]
                        
                        # Check if this is a multi-season pack
                        is_multi_season = any(indicator in title for indicator in multi_season_indicators)
                        
                        # Additional check for patterns like "S01S02", "Season1Season2", etc.
                        import re
                        multi_season_patterns = [
                            r's\d+s\d+',  # S01S02, S1S2
                            r'season\s*\d+\s*season\s*\d+',  # Season1Season2, Season 1 Season 2
                            r's\d+-\d+',  # S01-02, S1-2
                            r'season\s*\d+-\d+',  # Season1-2, Season 1-2
                        ]
                        
                        for pattern in multi_season_patterns:
                            if re.search(pattern, title, re.IGNORECASE):
                                is_multi_season = True
                                break
                        
                        if not is_multi_season:
                            single_season_packs.append(release)
                        else:
                            logger.info(f"Pack multi-saisons rejeté : {release.get('title', 'Unknown')}")
                    
                    logger.info(f"Filtré à {len(single_season_packs)} Packs d’une seule saison (rejected {len(season_packs) - len(single_season_packs)} multi-season packs)")
                    
                    return single_season_packs
                else:
                    logger.warning(f"L’API des Releases a renvoyé {response.status_code}: {response.text}")
                    return []
                    
            except httpx.ReadTimeout:
                logger.warning(f"Délai d’attente dépassé lors de la récupération des Releases pour la série {series_id} saison {season_number} (attempt {attempt + 1})")
                if attempt < max_retries:
                    await asyncio.sleep(5)  # Wait before retry
                    continue
                else:
                    logger.error(f"Délai d’attente final après {max_retries + 1} tentatives. Sonarr est peut-être surchargé.")
                    return []
            except httpx.RequestError as e:
                logger.error(f"NErreur réseau lors de la récupération des Releases pour la série {series_id} saison {season_number} (attempt {attempt + 1}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(5)  # Wait before retry
                    continue
                else:
                    return []
            except Exception as e:
                import traceback
                logger.error(f"Error fetching releases (attempt {attempt + 1}): {e}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                if attempt < max_retries:
                    await asyncio.sleep(5)  # Wait before retry
                    continue
                else:
                    return []

    async def get_missing_episodes(self, series_id: int, season_number: int = None) -> Dict[str, Any]:
        """Get missing episodes for a series, optionally filtered by season"""
        try:
            async with httpx.AsyncClient() as client:
                params = {"seriesId": series_id}
                response = await client.get(
                    f"{self.base_url}/api/v3/episode",
                    params=params,
                    headers=self.headers,
                    timeout=30.0
                )
                
                if response.status_code != 200:
                    raise Exception(f"Échec de la récupération des épisodes : {response.status_code}")
                
                episodes = response.json()
                logger.info(f"Récupération {len(episodes)} episodes pour la série {series_id}")
                
                # Filter episodes based on criteria
                missing_episodes = []
                for episode in episodes:
                    # Skip if season_number is specified and doesn't match
                    if season_number is not None and episode.get("seasonNumber") != season_number:
                        continue
                    
                    # Skip specials (season 0)
                    if episode.get("seasonNumber", 0) <= 0:
                        continue
                    
                    # Check if episode is missing
                    # An episode is missing if it's monitored and doesn't have a file
                    if (episode.get("monitored", False) and 
                        not episode.get("hasFile", False)):
                        missing_episodes.append(episode)
                
                # Group by season
                seasons_with_missing = {}
                for episode in missing_episodes:
                    season_num = episode.get("seasonNumber")
                    if season_num not in seasons_with_missing:
                        seasons_with_missing[season_num] = []
                    seasons_with_missing[season_num].append(episode)
                
                return {
                    "total_missing": len(missing_episodes),
                    "seasons_with_missing": seasons_with_missing,
                    "episodes": missing_episodes
                }
                
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des épisodes manquants : {e}")
            raise

    async def has_future_episodes(self, series_id: int, season_number: int = None) -> Dict[str, Any]:
        """Check if a season has episodes that haven't aired yet"""
        try:
            from datetime import datetime, timezone
            
            async with httpx.AsyncClient() as client:
                params = {"seriesId": series_id}
                response = await client.get(
                    f"{self.base_url}/api/v3/episode",
                    params=params,
                    headers=self.headers,
                    timeout=30.0
                )
                
                if response.status_code != 200:
                    raise Exception(f"Échec de la récupération des épisodes : {response.status_code}")
                
                episodes = response.json()
                now = datetime.now(timezone.utc)
                
                # Group episodes by season and check for future air dates
                seasons_with_future = {}
                seasons_complete = {}
                
                for episode in episodes:
                    season_num = episode.get("seasonNumber", 0)
                    
                    # Skip specials (season 0)
                    if season_num <= 0:
                        continue
                    
                    # Skip if season_number is specified and doesn't match
                    if season_number is not None and season_num != season_number:
                        continue
                    
                    # Only check monitored episodes
                    if not episode.get("monitored", False):
                        continue
                    
                    # Initialize season tracking
                    if season_num not in seasons_with_future:
                        seasons_with_future[season_num] = []
                        seasons_complete[season_num] = True
                    
                    # Check if episode has future air date
                    air_date_str = episode.get("airDateUtc") or episode.get("airDate")
                    if air_date_str:
                        try:
                            # Parse air date
                            if "T" in air_date_str:
                                air_date = datetime.fromisoformat(air_date_str.replace("Z", "+00:00"))
                            else:
                                air_date = datetime.fromisoformat(air_date_str + "T00:00:00+00:00")
                            
                            # If episode hasn't aired yet, this season is not complete
                            if air_date > now:
                                seasons_with_future[season_num].append(episode)
                                seasons_complete[season_num] = False
                        except ValueError:
                            # If we can't parse the date, assume it's already aired
                            pass
                
                # Filter out seasons that have future episodes
                complete_seasons = {
                    season_num: complete 
                    for season_num, complete in seasons_complete.items() 
                    if complete
                }
                
                return {
                    "seasons_with_future": seasons_with_future,
                    "seasons_complete": list(complete_seasons.keys()),
                    "seasons_incomplete": [s for s, complete in seasons_complete.items() if not complete]
                }
                
        except Exception as e:
            logger.error(f"Error checking future episodes: {e}")
            raise

    async def delete_season_episodes(self, series_id: int, season_number: int):
        try:
            async with httpx.AsyncClient() as client:
                episodes_response = await client.get(
                    f"{self.base_url}/api/v3/episode",
                    params={"seriesId": series_id},
                    headers=self.headers
                )
                
                if episodes_response.status_code != 200:
                    raise Exception("Failed to fetch episodes")
                
                episodes = episodes_response.json()
                season_episodes = [
                    ep for ep in episodes 
                    if ep.get("seasonNumber") == season_number and ep.get("hasFile", False)
                ]
                
                for episode in season_episodes:
                    if episode.get("episodeFileId"):
                        await client.delete(
                            f"{self.base_url}/api/v3/episodefile/{episode['episodeFileId']}",
                            headers=self.headers
                        )
                        
        except Exception as e:
            logger.error(f"Error deleting season episodes: {e}")
            raise

    async def grab_release(self, release_guid: str):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/api/v3/release",
                    headers=self.headers,
                    json={"guid": release_guid}
                )
                
                if response.status_code not in [200, 201]:
                    raise Exception(f"Failed to grab release: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"Error grabbing release: {e}")
            raise

    async def download_release_direct(self, release_guid: str, indexer_id: int):
        """Download a specific release by GUID and indexer_id directly"""
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
                logger.info(f"Téléchargement de la sortie avec le GUID : {release_guid} et indexer_id : {indexer_id}")
                
                # Validate indexer_id
                if not indexer_id or indexer_id <= 0:
                    raise Exception(f"indexer_id invalide : {indexer_id}")
                
                # Download the release with both guid and indexerId
                response = await client.post(
                    f"{self.base_url}/api/v3/release",
                    headers=self.headers,
                    json={"guid": release_guid, "indexerId": indexer_id}
                )
                
                if response.status_code not in [200, 201]:
                    logger.error(f"Échec du téléchargement de la sortie : {response.status_code} - {response.text}")
                    raise Exception(f"Échec du téléchargement de la sortie : {response.status_code}")
                
                logger.info(f"Release download initiated successfully")
                return response.json()
                    
        except Exception as e:
            logger.error(f"Erreur lors du téléchargement de la sortie : {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def download_release(self, release_guid: str, series_id: int = None, season_number: int = None):
        """Download a specific release by GUID"""
        try:
            # First, get the release details to find the indexerId
            async with httpx.AsyncClient() as client:
                logger.info(f"Téléchargement de la sortie avec le GUID : {release_guid}")
                
                # Get releases for the specific series and season to find the one with matching GUID
                logger.info(f"Obtention des Releases pour la série {series_id} saison {season_number} pour trouver l’indexerId correspondant au GUID : {release_guid}")
                
                # Use the same parameters as the search to get the same release list
                params = {}
                if series_id:
                    params["seriesId"] = series_id
                if season_number:
                    params["seasonNumber"] = season_number
                
                releases_response = await client.get(
                    f"{self.base_url}/api/v3/release",
                    params=params,
                    headers=self.headers
                )
                
                if releases_response.status_code != 200:
                    logger.error(f"Failed to get releases: {releases_response.status_code}")
                    raise Exception(f"Failed to get releases: {releases_response.status_code}")
                
                releases = releases_response.json()
                logger.info(f"Found {len(releases)} total releases, searching for GUID: {release_guid}")
                target_release = None
                
                # Find the release with matching GUID
                for release in releases:
                    if release.get("guid") == release_guid:
                        target_release = release
                        logger.info(f"Sortie cible trouvée avec l’indexerId : {release.get('indexerId')}")
                        break
                
                if not target_release:
                    logger.error(f"Release with GUID {release_guid} not found in {len(releases)} releases")
                    raise Exception(f"Release with GUID {release_guid} not found")
                
                # Extract required fields
                indexer_id = target_release.get("indexerId")
                if not indexer_id or indexer_id <= 0:
                    raise Exception(f"Invalid indexerId: {indexer_id}")
                
                # Now download the release with both guid and indexerId
                response = await client.post(
                    f"{self.base_url}/api/v3/release",
                    headers=self.headers,
                    json={"guid": release_guid, "indexerId": indexer_id}
                )
                
                if response.status_code not in [200, 201]:
                    logger.error(f"Échec du téléchargement release : {response.status_code} - {response.text}")
                    raise Exception(f"Échec du téléchargement de la release: {response.status_code}")
                
                logger.info(f"Téléchargement de la Release démarré avec succès")
                return response.json()
                    
        except Exception as e:
            logger.error(f"Error downloading release: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise


async def test_sonarr_connection(url: str, api_key: str) -> bool:
    try:
        import httpx
        
        headers = {
            "X-Api-Key": api_key,
            "Content-Type": "application/json"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{url.rstrip('/')}/api/v3/system/status",
                headers=headers,
                timeout=10.0
            )
            return response.status_code == 200
            
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False