import discord
from src.operation_file.logger import Logger
from typing import Optional, Callable
import asyncio
import time
import io
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import json
import re
from datetime import datetime
import base64
import os


def load_or_create_config(file_path="config.json"):
    # Default settings
    defaults = {
        "CLONE_UPDATE_NAME_ICON": False
    }

    # Create file if it doesn't exist
    if not os.path.exists(file_path):
        with open(file_path, "w") as f:
            json.dump(defaults, f, indent=4)
        print(f"[INFO] {file_path} created with default values.")

    # Load JSON
    with open(file_path, "r") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            print(f"[ERROR] Invalid JSON in {file_path}, using defaults.")
            config = defaults

    # Ensure all default keys exist
    for key, value in defaults.items():
        if key not in config:
            config[key] = value

    return config


class Clone:
    def __init__(self, debug_callback=None):
        self.logger = Logger(debug_callback)
        self.total_roles = 0
        self.total_channels = 0
        self.total_messages = 0
        self.roles_created = 0
        self.channels_created = 0
        self.messages_copied = 0
        self.errors = 0
        self.start_time = None
        self.channel_map = {}  # Map to track old->new channels
        self.executor = ThreadPoolExecutor(max_workers=3)  # For async file operations
        self.progress_callback = None  # Callback per l'aggiornamento della barra di progresso
        self.stats = {
            "roles_created": 0,
            "categories_created": 0,
            "text_channels_created": 0,
            "voice_channels_created": 0,
            "forum_channels_created": 0,
            "messages_cloned": 0,
            "errors": 0,
            "start_time": None,
            "elapsed_time": 0
        }
        self.total_operations = 0
        self.completed_operations = 0
        self.roles_map = {}
        self.categories_map = {}
        self.channels_map = {}
        self.forum_map = {}  # Track forum channels

    def set_progress_callback(self, callback: Callable[[float], None]):
        """Imposta una callback per aggiornare l'UI con il progresso
        La callback riceve un valore da 0.0 a 1.0 che rappresenta la percentuale di completamento
        """
        self.progress_callback = callback

    def _update_progress(self, progress: float):
        """Aggiorna il progresso chiamando la callback se disponibile"""
        if self.progress_callback:
            # Assicuriamoci che il valore sia tra 0 e 1
            progress = max(0.0, min(1.0, progress))
            self.progress_callback(progress)

    async def start_clone(self, guild_from, guild_to, session, options=None) -> bool:
        """Start the cloning process with options using REST API
        
        Args:
            guild_from: JSON data of source guild
            guild_to: JSON data of destination guild
            session: aiohttp ClientSession with appropriate headers
            options: Dictionary of options to customize the cloning process:
                - clone_roles: Whether to clone roles
                - clone_categories: Whether to clone categories
                - clone_text_channels: Whether to clone text channels
                - clone_voice_channels: Whether to clone voice channels
                - clone_forum_channels: Whether to clone forum channels
                - clone_messages: Whether to clone messages
                - messages_limit: Maximum number of messages to clone per channel
                - clone_name_icon: clones the name and icon of the destined server
        """
        try:
            self.start_time = time.time()
            
            # Reset statistics
            self.stats = {
                "roles_created": 0,
                "categories_created": 0,
                "text_channels_created": 0,
                "voice_channels_created": 0,
                "forum_channels_created": 0,
                "messages_cloned": 0,
                "errors": 0,
                "start_time": time.time(),
                "elapsed_time": 0
            }
            
            # Reset entity maps
            self.roles_map = {}
            self.categories_map = {}
            self.channels_map = {}
            self.forum_map = {}
            
            # Default options if none provided
            if options is None:
                options = {
                    "clone_roles": True,
                    "clone_categories": True,
                    "clone_text_channels": True,
                    "clone_voice_channels": True,
                    "clone_forum_channels": True,
                    "clone_messages": True,
                    "messages_limit": 100,
                    "clone_name_icon": False
                }
         
            # Calculate total operations for progress tracking
            self.total_operations = 0
            self.completed_operations = 0
            
            source_id = guild_from.get("id")
            dest_id = guild_to.get("id")
            
            self._safe_log(f"Starting cloning process from {guild_from.get('name')} to {guild_to.get('name')}")
            
            # Fetch all data from the source server
            
            # Roles
            roles_url = f"https://discord.com/api/v10/guilds/{source_id}/roles"
            roles_data = []
            
            if options.get("clone_roles", True):
                async with session.get(roles_url) as roles_response:
                    if roles_response.status == 200:
                        roles_data = await roles_response.json()
                        # Filtriamo il ruolo everyone che non possiamo clonare
                        roles_data = [r for r in roles_data if r.get("name") != "@everyone"]
                        self.total_roles = len(roles_data)
                        self._safe_log(f"Found {self.total_roles} roles to clone")
                    else:
                        self._safe_log(f"Error fetching roles: {roles_response.status}", "ERROR")
                        self.errors += 1
            
            # Canali
            channels_url = f"https://discord.com/api/v10/guilds/{source_id}/channels"
            categories_data = []
            text_channels_data = []
            voice_channels_data = []
            forum_channels_data = []
            
            async with session.get(channels_url) as channels_response:
                if channels_response.status == 200:
                    all_channels = await channels_response.json()
                    
                    # Dividiamo i canali per tipo
                    categories_data = [c for c in all_channels if c.get("type") == 4]
                    text_channels_data = [c for c in all_channels if c.get("type") == 0]
                    voice_channels_data = [c for c in all_channels if c.get("type") == 2]
                    forum_channels_data = [c for c in all_channels if c.get("type") == 15]  # Forum type
                    
                    total_channels = 0
                    if options.get("clone_categories", True):
                        total_channels += len(categories_data)
                    if options.get("clone_text_channels", True):
                        total_channels += len(text_channels_data)
                    if options.get("clone_voice_channels", True):
                        total_channels += len(voice_channels_data)
                    if options.get("clone_forum_channels", True):
                        total_channels += len(forum_channels_data)
                    
                    self.total_channels = total_channels
                    self._safe_log(f"Found {self.total_channels} channels to clone (including {len(forum_channels_data)} forums)")
                else:
                    self._safe_log(f"Error fetching channels: {channels_response.status}", "ERROR")
                    self.errors += 1
            
            # Inizializziamo il progresso
            self._update_progress(0.0)
            
            # Customize progress percentages based on selected options
            progress_steps = []
            current_progress = 0.0
            
            # Basic server settings always come first
            progress_steps.append(("edit_guild", 0.05))
            current_progress += 0.05
            
            # Calculate progress percentages for each step
            # We allocate percentages based on whether an option is enabled
            
            if options.get("clone_roles", True):
                progress_steps.append(("delete_roles", current_progress + 0.10))
                current_progress += 0.10
                progress_steps.append(("create_roles", current_progress + 0.15))
                current_progress += 0.15
            
            if options.get("clone_categories", True) or options.get("clone_text_channels", True) or options.get("clone_voice_channels", True) or options.get("clone_forum_channels", True):
                progress_steps.append(("delete_channels", current_progress + 0.10))
                current_progress += 0.10
            
            if options.get("clone_categories", True):
                progress_steps.append(("create_categories", current_progress + 0.10))
                current_progress += 0.10
            
            channel_progress = 0.0
            if options.get("clone_text_channels", True) or options.get("clone_voice_channels", True) or options.get("clone_forum_channels", True):
                channel_progress = 0.20
                progress_steps.append(("create_channels", current_progress + channel_progress))
                current_progress += channel_progress
            
            if options.get("clone_messages", True):
                progress_steps.append(("copy_messages", 1.0))  # Messages always go to 100%
            else:
                # If we don't clone messages, we need to ensure we reach 100%
                self._update_progress(1.0)
            
            # Store progress steps for reference
            self.progress_steps = dict(progress_steps)

            # Cloning sequence with options

            # Basic server settings (name, icon)
            await self._edit_guild_rest(guild_to, guild_from, session, options=options)
            self._update_progress(self.progress_steps.get("edit_guild", 0.05))

            if options.get("clone_roles", True) and roles_data:
                await self._delete_existing_roles_rest(guild_to, session)
                self._update_progress(self.progress_steps.get("delete_roles", 0.15))
                
                await self._create_roles_rest(guild_to, roles_data, session)
                self._update_progress(self.progress_steps.get("create_roles", 0.30))
            
            # Channels
            channel_types_to_clone = []
            if options.get("clone_categories", True):
                channel_types_to_clone.append("categories")
            if options.get("clone_text_channels", True):
                channel_types_to_clone.append("text")
            if options.get("clone_voice_channels", True):
                channel_types_to_clone.append("voice")
            if options.get("clone_forum_channels", True):
                channel_types_to_clone.append("forum")
            
            if channel_types_to_clone:
                await self._delete_existing_channels_rest(guild_to, session)
                self._update_progress(self.progress_steps.get("delete_channels", 0.40))
            
            # Use the combined create function
            if (options.get("clone_categories", True) and categories_data) or \
               (options.get("clone_text_channels", True) and text_channels_data) or \
               (options.get("clone_voice_channels", True) and voice_channels_data) or \
               (options.get("clone_forum_channels", True) and forum_channels_data):

                await self._create_categories_channels_forums_rest(
                    guild_to,
                    categories_data if options.get("clone_categories", True) else [],
                    text_channels_data if options.get("clone_text_channels", True) else [],
                    voice_channels_data if options.get("clone_voice_channels", True) else [],
                    forum_channels_data if options.get("clone_forum_channels", True) else [],
                    session
                )
                # Update progress in one go (or split if you want finer progress tracking)
                self._update_progress(self.progress_steps.get("create_channels", 0.70))
            
            # Clone forum threads and messages
            if options.get("clone_messages", True) and forum_channels_data:
                self._safe_log("Starting forum thread cloning...")
                await self._clone_forum_threads(guild_to, forum_channels_data, session, options.get("messages_limit", 100))
                self._update_progress(self.progress_steps.get("copy_messages", 0.90))

            elapsed = time.time() - self.start_time
            self.logger.add(f"Cloning completed in {elapsed:.2f} seconds")
            return True

        except Exception as e:
            self.logger.error(f"Critical error during cloning: {str(e)}")
            return False

    async def _edit_guild_rest(self, guild_to, guild_from, session, options=None):
        try:
            if options is not None:
                clone_name_icon = options.get("clone_name_icon", False)

            if not clone_name_icon:
                self._safe_log("Skipping guild name/icon update (option disabled)")
                return

            payload = {"name": guild_from.get("name")}

            # Copy the icon if present
            icon_hash = guild_from.get("icon")
            if icon_hash:
                icon_url = f"https://cdn.discordapp.com/icons/{guild_from.get('id')}/{icon_hash}.png"
                async with session.get(icon_url) as resp:
                    if resp.status == 200:
                        icon_bytes = await resp.read()
                        payload["icon"] = f"data:image/png;base64,{base64.b64encode(icon_bytes).decode()}"

            async with session.patch(f"https://discord.com/api/v10/guilds/{guild_to.get('id')}", json=payload) as resp:
                if resp.status in [200, 201]:
                    self._safe_log("Guild name/icon updated successfully")
                else:
                    self._safe_log(f"Failed updating guild: {resp.status}", "ERROR")

        except Exception as e:
            self._safe_log(f"Error updating guild: {str(e)}", "ERROR")

    async def _delete_existing_roles_rest(self, guild_to, session):
        """Delete all existing roles except @everyone using REST API"""
        self._safe_log("Deleting existing roles...")
        try:
            roles_url = f"https://discord.com/api/v10/guilds/{guild_to.get('id')}/roles"
            async with session.get(roles_url) as resp:
                if resp.status == 200:
                    roles_data = await resp.json()
                    for role in roles_data:
                        if role.get("name") == "@everyone":
                            continue
                        try:
                            async with session.delete(f"{roles_url}/{role.get('id')}") as del_resp:
                                if del_resp.status in (200, 204):
                                    self._safe_log(f"Deleted role: {role.get('name')}")
                                elif del_resp.status == 429:
                                    rate_limit_data = await del_resp.json()
                                    retry_after = rate_limit_data.get("retry_after", 5)
                                    self._safe_log(f"Rate limit hit deleting role {role.get('name')}, waiting {retry_after}s", "ERROR")
                                    await asyncio.sleep(retry_after)
                                    continue
                                else:
                                    self.errors += 1
                                    self._safe_log(f"Error deleting role {role.get('name')}: {del_resp.status}", "ERROR")
                            await asyncio.sleep(1.0)
                        except Exception as e:
                            self.errors += 1
                            self._safe_log(f"Exception deleting role {role.get('name')}: {str(e)}", "ERROR")
                            await asyncio.sleep(2.0)
                else:
                    self.errors += 1
                    self._safe_log(f"Failed to fetch roles for deletion: {resp.status}", "ERROR")
        except Exception as e:
            self.errors += 1
            self._safe_log(f"Critical error deleting roles: {str(e)}", "ERROR")

    async def _delete_existing_channels_rest(self, guild_to, session):
        """Delete all existing channels using REST API (properly by ID)"""
        self._safe_log("Deleting existing channels...")

        try:
            # Fetch all channels from the target guild
            async with session.get(f"https://discord.com/api/v10/guilds/{guild_to.get('id')}/channels") as resp:
                if resp.status != 200:
                    self.errors += 1
                    self._safe_log(f"Failed to fetch channels for deletion: {resp.status}", "ERROR")
                    return
                
                channels = await resp.json()

            # Sort categories first, then other channels
            # This ensures that text/voice channels under categories get deleted properly
            categories = [c for c in channels if c.get("type") == 4]
            other_channels = [c for c in channels if c.get("type") != 4]

            all_channels_sorted = other_channels + categories  # Delete child channels first, then categories

            for channel in all_channels_sorted:
                channel_id = channel.get("id")
                channel_name = channel.get("name", "Unknown")

                try:
                    async with session.delete(f"https://discord.com/api/v10/channels/{channel_id}") as del_resp:
                        if del_resp.status in (200, 204):
                            self._safe_log(f"Deleted channel: {channel_name}")
                        elif del_resp.status == 429:  # Rate limit
                            rate_limit_data = await del_resp.json()
                            retry_after = rate_limit_data.get("retry_after", 5)
                            self._safe_log(f"Rate limit hit deleting channel {channel_name}, waiting {retry_after}s", "ERROR")
                            await asyncio.sleep(retry_after)
                            # Retry deletion
                            continue
                        else:
                            self.errors += 1
                            self._safe_log(f"Error deleting channel {channel_name}: {del_resp.status}", "ERROR")
                except Exception as e:
                    self.errors += 1
                    self._safe_log(f"Exception deleting channel {channel_name}: {str(e)}", "ERROR")
                
                # Small delay to avoid hitting rate limits
                await asyncio.sleep(1.0)
        
        except Exception as e:
            self.errors += 1
            self._safe_log(f"Critical error deleting channels: {str(e)}", "ERROR")

    def _safe_log(self, message: str, level: str = "INFO"):
        """Thread-safe logging wrapper"""
        try:
            if level == "ERROR":
                self.logger.error(message)
            else:
                self.logger.add(message)
        except Exception:
            pass

    async def _create_roles_rest(self, guild_to, roles_data, session):
        """Create new roles using REST API (POST, aggiorna mappa ID)"""
        self._safe_log("Creating new roles...")
        for role in roles_data:
            try:
                payload = {
                    "name": role.get('name'),
                    "permissions": role.get('permissions'),
                    "color": role.get('color'),
                    "hoist": role.get('hoist'),
                    "mentionable": role.get('mentionable')
                }
                async with session.post(f"https://discord.com/api/v10/guilds/{guild_to.get('id')}/roles", json=payload) as resp:
                    if resp.status == 200 or resp.status == 201:
                        created = await resp.json()
                        self.roles_map[role.get('id')] = created.get('id')
                        self.roles_created += 1
                        self._safe_log(f"Role created ({self.roles_created}/{self.total_roles}): {role.get('name')}")
                    elif resp.status == 429:  # Rate limit
                        # Estraiamo le informazioni sul rate limit
                        rate_limit_data = await resp.json()
                        retry_after = rate_limit_data.get('retry_after', 5)  # Default 5 secondi
                        self._safe_log(f"Rate limit hit when creating role {role.get('name')}. Waiting {retry_after} seconds...", "ERROR")
                        await asyncio.sleep(retry_after)
                        # Riprova la creazione del ruolo (decrementiamo l'indice del loop)
                        continue
                    else:
                        self.errors += 1
                        self._safe_log(f"Error creating role {role.get('name')}: {resp.status}", "ERROR")
                
                # Aggiungiamo un piccolo delay per evitare rate limits
                await asyncio.sleep(0.5)
            except Exception as e:
                self.errors += 1
                self._safe_log(f"Error creating role {role.get('name')}: {str(e)}", "ERROR")
                # In caso di errore, aspettiamo un po' di più
                await asyncio.sleep(1.0)

    async def _create_categories_channels_forums_rest(self, guild_to, categories_data, text_channels_data, voice_channels_data, forum_channels_data, session):
        """Create categories, channels, and forums preserving their layout and parent relationships"""
        
        # ---------------- CREATE CATEGORIES ----------------
        self._safe_log("Creating categories...")
        categories_data = sorted(categories_data, key=lambda c: c.get("position", 0))
        
        for category in categories_data:
            try:
                overwrites_to = []
                for role_id, perms in category.get("overwrites", {}).items():
                    overwrites_to.append({
                        "id": str(role_id),
                        "type": 0,  # role overwrite
                        "allow": perms.get("allow", "0"),
                        "deny": perms.get("deny", "0")
                    })

                payload = {
                    "name": category.get("name"),
                    "type": 4,  # 4 = category
                    "permission_overwrites": overwrites_to,
                    "position": category.get("position", 0)
                }

                async with session.post(
                    f"https://discord.com/api/v10/guilds/{guild_to.get('id')}/channels",
                    json=payload
                ) as response:
                    if response.status in (200, 201):
                        created = await response.json()
                        self.categories_map[category.get("id")] = created.get("id")
                        self._safe_log(f"Category created: {category.get('name')}")
                    elif response.status == 429:
                        rate_limit_data = await response.json()
                        retry_after = rate_limit_data.get("retry_after", 5)
                        self._safe_log(f"Rate limit hit creating category {category.get('name')}, waiting {retry_after}s", "ERROR")
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        self.errors += 1
                        self._safe_log(f"Error creating category {category.get('name')}: {response.status}", "ERROR")

                await asyncio.sleep(1.5)

            except Exception as e:
                self.errors += 1
                self._safe_log(f"Exception creating category {category.get('name')}: {str(e)}", "ERROR")
                await asyncio.sleep(3.0)

        # ---------------- CREATE TEXT CHANNELS ----------------
        self._safe_log("Creating text channels...")
        text_channels_data = sorted(text_channels_data, key=lambda c: c.get("position", 0))
        
        for channel in text_channels_data:
            try:
                payload = {
                    "name": channel.get("name"),
                    "type": 0,  # text
                    "topic": channel.get("topic"),
                    "position": channel.get("position", 0),
                    "nsfw": channel.get("nsfw", False),
                    "rate_limit_per_user": channel.get("slowmode_delay", 0)
                }

                # Map parent category
                old_cat_id = channel.get("category_id") or channel.get("parent_id")
                if old_cat_id and old_cat_id in self.categories_map:
                    payload["parent_id"] = str(self.categories_map[old_cat_id])

                # Permission overwrites
                overwrites_to = []
                for role_id, perms in channel.get("overwrites", {}).items():
                    overwrites_to.append({
                        "id": str(role_id),
                        "type": 0,
                        "allow": perms.get("allow", "0"),
                        "deny": perms.get("deny", "0")
                    })
                if overwrites_to:
                    payload["permission_overwrites"] = overwrites_to

                self._safe_log(f"Creating text channel {channel.get('name')} under category {payload.get('parent_id')}")
                
                async with session.post(
                    f"https://discord.com/api/v10/guilds/{guild_to.get('id')}/channels",
                    json=payload
                ) as resp:
                    if resp.status in (200, 201):
                        created = await resp.json()
                        self.channels_map[channel.get("id")] = created.get("id")
                        self._safe_log(f"Text channel created: {channel.get('name')}")
                    elif resp.status == 429:
                        rate_limit_data = await resp.json()
                        retry_after = rate_limit_data.get("retry_after", 5)
                        self._safe_log(f"Rate limit hit creating text channel {channel.get('name')}, waiting {retry_after}s", "ERROR")
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        self.errors += 1
                        self._safe_log(f"Error creating text channel {channel.get('name')}: {resp.status}", "ERROR")

                await asyncio.sleep(1.5)

            except Exception as e:
                self.errors += 1
                self._safe_log(f"Exception creating text channel {channel.get('name')}: {str(e)}", "ERROR")
                await asyncio.sleep(3.0)

        # ---------------- CREATE VOICE CHANNELS ----------------
        self._safe_log("Creating voice channels...")
        voice_channels_data = sorted(voice_channels_data, key=lambda c: c.get("position", 0))
        
        for channel in voice_channels_data:
            try:
                payload = {
                    "name": channel.get("name"),
                    "type": 2,  # voice
                    "position": channel.get("position", 0),
                    "bitrate": channel.get("bitrate", 64000),
                    "user_limit": channel.get("user_limit", 0)
                }

                old_cat_id = channel.get("category_id") or channel.get("parent_id")
                if old_cat_id and old_cat_id in self.categories_map:
                    payload["parent_id"] = str(self.categories_map[old_cat_id])

                overwrites_to = []
                for role_id, perms in channel.get("overwrites", {}).items():
                    overwrites_to.append({
                        "id": str(role_id),
                        "type": 0,
                        "allow": perms.get("allow", "0"),
                        "deny": perms.get("deny", "0")
                    })
                if overwrites_to:
                    payload["permission_overwrites"] = overwrites_to

                self._safe_log(f"Creating voice channel {channel.get('name')} under category {payload.get('parent_id')}")
                
                async with session.post(
                    f"https://discord.com/api/v10/guilds/{guild_to.get('id')}/channels",
                    json=payload
                ) as resp:
                    if resp.status in (200, 201):
                        created = await resp.json()
                        self.channels_map[channel.get("id")] = created.get("id")
                        self._safe_log(f"Voice channel created: {channel.get('name')}")
                    elif resp.status == 429:
                        rate_limit_data = await resp.json()
                        retry_after = rate_limit_data.get("retry_after", 5)
                        self._safe_log(f"Rate limit hit creating voice channel {channel.get('name')}, waiting {retry_after}s", "ERROR")
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        self.errors += 1
                        self._safe_log(f"Error creating voice channel {channel.get('name')}: {resp.status}", "ERROR")

                await asyncio.sleep(1.5)

            except Exception as e:
                self.errors += 1
                self._safe_log(f"Exception creating voice channel {channel.get('name')}: {str(e)}", "ERROR")
                await asyncio.sleep(3.0)

        # ---------------- CREATE FORUM CHANNELS ----------------
        if forum_channels_data:
            self._safe_log("Creating forum channels...")
            forum_channels_data = sorted(forum_channels_data, key=lambda c: c.get("position", 0))
            
            for channel in forum_channels_data:
                try:
                    payload = {
                        "name": channel.get("name"),
                        "type": 15,  # Forum channel
                        "topic": channel.get("topic", ""),
                        "nsfw": channel.get("nsfw", False),
                        "default_auto_archive_duration": channel.get("default_auto_archive_duration", 1440),
                        "position": channel.get("position", 0)
                    }

                    # Map parent category
                    old_cat_id = channel.get("category_id") or channel.get("parent_id")
                    if old_cat_id and old_cat_id in self.categories_map:
                        payload["parent_id"] = str(self.categories_map[old_cat_id])

                    # Permission overwrites
                    overwrites_to = []
                    for role_id, perms in channel.get("overwrites", {}).items():
                        overwrites_to.append({
                            "id": str(role_id),
                            "type": 0,
                            "allow": perms.get("allow", "0"),
                            "deny": perms.get("deny", "0")
                        })
                    if overwrites_to:
                        payload["permission_overwrites"] = overwrites_to

                    # Tags (if any)
                    if channel.get("available_tags"):
                        payload["available_tags"] = channel.get("available_tags")

                    self._safe_log(f"Creating forum channel {channel.get('name')}")
                    
                    async with session.post(
                        f"https://discord.com/api/v10/guilds/{guild_to.get('id')}/channels",
                        json=payload
                    ) as resp:
                        if resp.status in (200, 201):
                            created = await resp.json()
                            self.channels_map[channel.get("id")] = created.get("id")
                            self.forum_map[channel.get("id")] = created.get("id")
                            self.stats["forum_channels_created"] += 1
                            self._safe_log(f"Forum channel created: {channel.get('name')}")
                        elif resp.status == 429:
                            rate_limit_data = await resp.json()
                            retry_after = rate_limit_data.get("retry_after", 5)
                            self._safe_log(f"Rate limit hit creating forum {channel.get('name')}, waiting {retry_after}s", "ERROR")
                            await asyncio.sleep(retry_after)
                            continue
                        else:
                            self.errors += 1
                            self._safe_log(f"Error creating forum {channel.get('name')}: {resp.status}", "ERROR")

                    await asyncio.sleep(1.5)

                except Exception as e:
                    self.errors += 1
                    self._safe_log(f"Exception creating forum {channel.get('name')}: {str(e)}", "ERROR")
                    await asyncio.sleep(3.0)

    async def _clone_forum_threads(self, guild_to, forum_channels_data, session, message_limit=100):
        """Clone all threads from source forums to target forums"""
        self._safe_log("Starting forum thread cloning...")
        
        fake_user_id = 1491710353929670729  # Force all messages to this user
        
        for forum in forum_channels_data:
            source_forum_id = forum.get("id")
            forum_name = forum.get("name")
            
            # Get the target forum ID from the map
            target_forum_id = self.forum_map.get(source_forum_id)
            if not target_forum_id:
                self._safe_log(f"Target forum for '{forum_name}' not found, skipping", "ERROR")
                continue
            
            self._safe_log(f"Cloning threads from forum: {forum_name}")
            
            # Get active threads from source forum
            threads_url = f"https://discord.com/api/v10/channels/{source_forum_id}/threads/active"
            async with session.get(threads_url) as resp:
                if resp.status == 200:
                    threads_data = await resp.json()
                    thread_list = threads_data.get("threads", [])
                    
                    self._safe_log(f"Found {len(thread_list)} active threads in {forum_name}")
                    
                    for thread in thread_list:
                        thread_name = thread.get("name")
                        thread_id = thread.get("id")
                        
                        self._safe_log(f"Cloning thread: {thread_name}")
                        
                        # Create thread in target forum
                        payload = {
                            "name": thread_name,
                            "type": 11,  # Public thread
                            "auto_archive_duration": 1440
                        }
                        
                        async with session.post(
                            f"https://discord.com/api/v10/channels/{target_forum_id}/threads",
                            json=payload
                        ) as thread_resp:
                            if thread_resp.status in (200, 201):
                                new_thread = await thread_resp.json()
                                new_thread_id = new_thread.get("id")
                                
                                # Copy messages from source thread
                                await self._copy_thread_messages(
                                    session, thread_id, new_thread_id, message_limit, fake_user_id
                                )
                                self._safe_log(f"Thread '{thread_name}' cloned successfully")
                            else:
                                self._safe_log(f"Failed to create thread {thread_name}: {thread_resp.status}", "ERROR")
                            
                            await asyncio.sleep(1.5)
                else:
                    self._safe_log(f"Failed to fetch threads for {forum_name}: {resp.status}", "ERROR")

    async def _copy_thread_messages(self, session, source_thread_id, target_thread_id, message_limit=100, fake_user_id=1491710353929670729):
        """Copy messages from a source thread to a target thread"""
        self._safe_log(f"Copying messages from thread {source_thread_id}")
        
        messages_url = f"https://discord.com/api/v10/channels/{source_thread_id}/messages?limit={message_limit}"
        async with session.get(messages_url) as resp:
            if resp.status == 200:
                messages = await resp.json()
                messages.reverse()  # Oldest first
                
                msg_count = 0
                for msg in messages:
                    if msg.get("content"):
                        content = msg.get("content")
                        timestamp = datetime.fromtimestamp(int(msg.get("timestamp", "").split('T')[1].split(':')[0])).strftime("%d/%m/%Y %H:%M") if msg.get("timestamp") else ""
                        
                        # Force attribution to fake user
                        fake_msg = f"**<@{fake_user_id}>**: {content}"
                        
                        # Send to target thread
                        send_payload = {"content": fake_msg[:2000]}
                        async with session.post(
                            f"https://discord.com/api/v10/channels/{target_thread_id}/messages",
                            json=send_payload
                        ) as send_resp:
                            if send_resp.status in (200, 201):
                                msg_count += 1
                            else:
                                self._safe_log(f"Failed to send message: {send_resp.status}", "ERROR")
                            
                            await asyncio.sleep(1.5)  # Rate limit safety
                
                self._safe_log(f"Copied {msg_count} messages to thread")
            else:
                self._safe_log(f"Failed to fetch messages: {resp.status}", "ERROR")

    async def _copy_messages(self, guild_from: discord.Guild, guild_to: discord.Guild, message_limit=100):
        """Copy messages from source server channels with a limit"""
        self._safe_log("Starting message copy...")
        
        # Conteggio totale dei messaggi da copiare (approssimativo)
        # Per ogni canale di testo, utilizziamo il limite specificato
        self.total_messages = len(guild_from.text_channels) * message_limit
        
        copy_tasks = []
        for channel_from in guild_from.text_channels:
            channel_to = discord.utils.get(guild_to.text_channels, name=channel_from.name)
            if channel_to:
                copy_tasks.append(self._copy_channel_messages(channel_from, channel_to, message_limit))
        
        # Execute message copying concurrently in batches, using smaller batches
        batch_size = 2  # Ridotto da 3 a 2 per evitare rate limit
        for i in range(0, len(copy_tasks), batch_size):
            batch = copy_tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            
            # Aspettiamo un po' tra i batch per evitare rate limit
            await asyncio.sleep(1.0)

    async def _copy_channel_messages(self, channel_from, channel_to, message_limit=100):
        """Helper method for copying messages from one channel with a limit"""
        try:
            # Iniziamo con un piccolo delay tra i messaggi per evitare rate limit
            rate_limit_delay = 0.7
            consecutive_errors = 0
            message_count = 0
            
            # FORCE ALL MESSAGES TO BE ATTRIBUTED TO FAKE USER
            fake_user_id = 1491710353929670729
            
            # Recuperiamo i messaggi in ordine cronologico inverso (dal più recente)
            messages = []
            async for message in channel_from.history(limit=message_limit):
                messages.append(message)
            
            # Invertiamo per inviarli in ordine cronologico (dal più vecchio)
            messages.reverse()
            
            for message in messages:
                # Se abbiamo troppi errori consecutivi, aumentiamo molto il delay
                if consecutive_errors >= 3:
                    rate_limit_delay = min(5.0, rate_limit_delay * 1.5)
                    consecutive_errors = 0
                
                try:
                    timestamp = message.created_at.strftime("%d/%m/%Y %H:%M")
                    
                    # Gestione degli embed e del contenuto
                    content = f"**<@{fake_user_id}>** *{timestamp}*: {message.content}" if message.content else f"**<@{fake_user_id}>** *{timestamp}*"
                    
                    # Formattiamo il contenuto
                    if len(content) > 2000:
                        # Se il messaggio è troppo lungo, lo tronchiamo
                        content = content[:1997] + "..."
                    
                    # Inviamo il messaggio
                    if content:
                        await channel_to.send(content=content)
                        self.messages_copied += 1
                        message_count += 1
                        
                    # Gestione degli allegati
                    if message.attachments:
                        files = []
                        
                        # Utilizziamo una sessione sicura per scaricare gli allegati
                        connector = aiohttp.TCPConnector(force_close=True)
                        async with aiohttp.ClientSession(connector=connector) as session:
                            for attachment in message.attachments:
                                try:
                                    # Scarichiamo direttamente dall'URL dell'allegato
                                    async with session.get(attachment.url) as response:
                                        if response.status == 200:
                                            file_data = await response.read()
                                            files.append(discord.File(io.BytesIO(file_data), filename=attachment.filename))
                                except Exception:
                                    continue
                        
                        # Se abbiamo scaricato file, li inviamo
                        if files:
                            await channel_to.send(files=files)
                            self.messages_copied += 1
                            message_count += 1
                    
                    # Aspettiamo tra un messaggio e l'altro per evitare rate limit
                    await asyncio.sleep(rate_limit_delay)
                    
                    # Se tutto va bene, riduciamo gradualmente il delay
                    rate_limit_delay = max(0.5, rate_limit_delay * 0.95)
                    # Reset del contatore errori in caso di successo
                    consecutive_errors = 0
                except discord.errors.HTTPException as e:
                    if e.status == 429:  # Rate limit
                        consecutive_errors += 1
                        self._safe_log(f"Rate limit reached, waiting longer ({round(rate_limit_delay, 1)}s)")
                        await asyncio.sleep(rate_limit_delay)
                    else:
                        consecutive_errors += 1
                        self._safe_log(f"HTTP error while sending message: {e}")
                        await asyncio.sleep(rate_limit_delay)
                except Exception as e:
                    consecutive_errors += 1
                    self._safe_log(f"Error copying message: {str(e)}")
                    await asyncio.sleep(rate_limit_delay)
            
            self._safe_log(f"Copied {message_count} messages to {channel_to.name}")
            return message_count
        except Exception as e:
            self._safe_log(f"Error during message copying: {str(e)}")
            return 0

    def get_stats(self) -> dict:
        """Return cloning statistics"""
        # Update elapsed time if started
        if self.stats["start_time"]:
            self.stats["elapsed_time"] = time.time() - self.stats["start_time"]
        return self.stats
