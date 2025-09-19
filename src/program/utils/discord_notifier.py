import httpx
from datetime import datetime
from typing import List, Dict, Any, Optional
from loguru import logger


# Discord impose des limites
DISCORD_MAX_DESCRIPTION = 4000
DISCORD_MAX_FIELDS = 25


# ============================================================
#  🔔 Notification instantanée (événement critique)
# ============================================================

async def send_discord_message(
    webhook_url: str,
    title: str,
    description: str,
    color: int = 0x5865F2,
    module: Optional[str] = "Symlinks",
    action: Optional[str] = None
) -> None:
    """
    📡 Envoie une notification instantanée sur Discord (embed stylisé).
    action peut être: created | deleted | broken
    """
    if len(description) > DISCORD_MAX_DESCRIPTION:
        description = description[:DISCORD_MAX_DESCRIPTION] + "… (tronqué)"

    # 🎨 Couleur automatique selon l’action
    if action == "created":
        color = 0x2ECC71  # vert
    elif action == "deleted":
        color = 0xE74C3C  # rouge
    elif action == "broken":
        color = 0xE67E22  # orange ⚠️

    embed = {
        "title": title,
        "description": description,
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "footer": {
            "text": "SSDv2 • Media Manager",
            "icon_url": "https://cdn-icons-png.flaticon.com/512/5968/5968756.png"
        },
        "author": {
            "name": "Symlinks Bot",
            "icon_url": "https://cdn-icons-png.flaticon.com/512/4712/4712109.png"
        },
        "fields": [
            {
                "name": "📂 Module",
                "value": module,
                "inline": True
            },
            {
                "name": "🕒 Heure",
                "value": datetime.utcnow().strftime("%H:%M:%S UTC"),
                "inline": True
            }
        ]
    }

    # Ajout champ spécial si symlink brisé
    if action == "broken":
        embed["fields"].append({
            "name": "⚠️ Alerte",
            "value": "Un symlink brisé a été détecté (cible manquante).",
            "inline": False
        })

    payload = {"embeds": [embed]}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(webhook_url, json=payload)
            response.raise_for_status()
    except Exception as e:
        # ⚠️ Filtrer les rate limits Discord (429)
        if "429" in str(e):
            logger.trace("⏱️ Discord rate limit atteint → message ignoré")
        else:
            logger.error(f"❌ Erreur envoi Discord (message instantané): {e}")


# ============================================================
#  📊 Rapport groupé (premium style avec fields)
# ============================================================

async def send_discord_summary(
    webhook_url: str,
    events: List[Dict[str, Any]],
) -> None:
    """
    📊 Envoie un résumé groupé des événements symlinks sur Discord (premium fields style).
    Chaque symlink = 1 field (mini carte).
    """
    if not events:
        return

    # Compteurs
    created_count = sum(1 for e in events if e["action"] == "created")
    deleted_count = sum(1 for e in events if e["action"] == "deleted")
    broken_count  = sum(1 for e in events if e["action"] == "broken")

    # Regroupe en lots de 25 symlinks max
    chunks = [events[i:i + DISCORD_MAX_FIELDS] for i in range(0, len(events), DISCORD_MAX_FIELDS)]

    for chunk in chunks:
        fields = []

        for e in chunk:
            if e["action"] == "created":
                fields.append({
                    "name": "🟢 Créé",
                    "value": (
                        f"`{e['path']}`\n"
                        f"→ 🎯 `{e.get('target')}`\n"
                        f"⏰ {e['time'].strftime('%H:%M:%S')}"
                    ),
                    "inline": False
                })
            elif e["action"] == "deleted":
                fields.append({
                    "name": "🔴 Supprimé",
                    "value": (
                        f"`{e['path']}`\n"
                        f"⏰ {e['time'].strftime('%H:%M:%S')}"
                    ),
                    "inline": False
                })
            elif e["action"] == "broken":
                fields.append({
                    "name": "⚠️ Brisé",
                    "value": (
                        f"`{e['path']}`\n"
                        f"(cible manquante)\n"
                        f"⏰ {e['time'].strftime('%H:%M:%S')}"
                    ),
                    "inline": False
                })

        embed = {
            "title": "📊 Rapport Symlinks",
            "description": (
                f"✅ **{created_count} créés** | "
                f"❌ **{deleted_count} supprimés** | "
                f"⚠️ **{broken_count} brisés** | "
                f"📂 **{len(events)} total**"
            ),
            "color": 0x2ECC71 if created_count else (0xE74C3C if deleted_count else 0xE67E22),
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {
                "text": "SSDv2 • Symlinks Manager",
                "icon_url": "https://cdn-icons-png.flaticon.com/512/5968/5968756.png"
            },
            "author": {
                "name": "Symlinks Bot",
                "icon_url": "https://cdn-icons-png.flaticon.com/512/4712/4712109.png"
            },
            "fields": fields
        }

        payload = {"embeds": [embed]}

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(webhook_url, json=payload)
                response.raise_for_status()
        except Exception as e:
            # ⚠️ Filtrer les rate limits Discord (429)
            if "429" in str(e):
                logger.trace("⏱️ Discord rate limit atteint → message ignoré")
            else:
                logger.error(f"❌ Erreur envoi Discord (message instantané): {e}")
