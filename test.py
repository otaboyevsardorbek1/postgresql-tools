#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
POSTGRESQL ENTERPRISE MONITORING VA BOSHQARUV TIZIMI
Version: 3.0.0
Muallif: DevOps Team
Litsenziya: Proprietary

Ushbu dastur PostgreSQL databazalarini to'liq boshqarish va monitoring qilish
uchun mo'ljallangan. Barcha databazalar, userlar va ularning ruxsatlarini
markazlashtirilgan holda kuzatish imkonini beradi.
"""

import os
import sys
import json
import time
import psycopg2
import hashlib
import logging
import datetime
import threading
import subprocess
from typing import Dict, List, Tuple, Optional, Any
from contextlib import contextmanager
from dataclasses import dataclass
from prettytable import PrettyTable
from colorama import init, Fore, Back, Style
import getpass
import re

# Colorama ni ishga tushurish
init(autoreset=True)

# ============================================================================
# KONFIGURATSIYA
# ============================================================================

@dataclass
class Config:
    """Asosiy konfiguratsiya"""
    LOG_DIR: str = "/var/log/pg_enterprise"
    LOG_FILE: str = f"/var/log/pg_enterprise/pg_manager_{datetime.datetime.now().strftime('%Y%m%d')}.log"
    BACKUP_DIR: str = "/var/backups/postgresql"
    CONFIG_BACKUP_DIR: str = "/etc/postgresql/backups"
    MONITOR_INTERVAL: int = 5
    MAX_CONNECTIONS: int = 100
    TIMEOUT: int = 30
    
    # Ranglar
    COLORS = {
        'HEADER': '\033[95m',
        'OKBLUE': '\033[94m',
        'OKCYAN': '\033[96m',
        'OKGREEN': '\033[92m',
        'WARNING': '\033[93m',
        'FAIL': '\033[91m',
        'ENDC': '\033[0m',
        'BOLD': '\033[1m',
        'UNDERLINE': '\033[4m'
    }

config = Config()

# ============================================================================
# LOGGER SOZLASH
# ============================================================================

class Logger:
    """Logging tizimi"""
    
    def __init__(self):
        os.makedirs(config.LOG_DIR, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(config.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('PostgreSQL_Manager')
    
    def info(self, message):
        self.logger.info(f"{Fore.GREEN}✓ {message}{Style.RESET_ALL}")
    
    def warning(self, message):
        self.logger.warning(f"{Fore.YELLOW}⚠ {message}{Style.RESET_ALL}")
    
    def error(self, message):
        self.logger.error(f"{Fore.RED}✗ {message}{Style.RESET_ALL}")
    
    def success(self, message):
        self.logger.info(f"{Fore.GREEN}✅ {message}{Style.RESET_ALL}")
    
    def debug(self, message):
        self.logger.debug(f"{Fore.CYAN}🔍 {message}{Style.RESET_ALL}")

logger = Logger()

# ============================================================================
# POSTGRESQL BOSHQARUVCHI
# ============================================================================

class PostgreSQLManager:
    """PostgreSQL ni to'liq boshqarish uchun asosiy klass"""
    
    def __init__(self, host='localhost', port=5432, user='postgres', password=None):
        self.host = host
        self.port = port
        self.superuser = user
        self.superuser_password = password or self._get_superuser_password()
        self.connection = None
        self.databases = {}
        self.users = {}
        self.monitoring_active = False
        self.monitor_thread = None
        
    def _get_superuser_password(self):
        """Superuser parolini olish"""
        if os.path.exists('/etc/postgresql/superuser.pwd'):
            with open('/etc/postgresql/superuser.pwd', 'r') as f:
                return f.read().strip()
        return None
    
    @contextmanager
    def get_connection(self, dbname='postgres', user=None, password=None):
        """Database ulanishni yaratish"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=dbname,
                user=user or self.superuser,
                password=password or self.superuser_password,
                connect_timeout=config.TIMEOUT
            )
            conn.autocommit = True
            yield conn
        except Exception as e:
            logger.error(f"Ulanish xatosi: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    # ========================================================================
    # DATABASE BOSHQARISH
    # ========================================================================
    
    def create_database(self, db_name: str, owner: str = None, encoding: str = 'UTF8', 
                       template: str = 'template0', tablespace: str = None) -> bool:
        """
        Yangi database yaratish
        
        Args:
            db_name: Database nomi
            owner: Database egasi (user)
            encoding: Kodirovka
            template: Template database
            tablespace: Tablespace nomi
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Database mavjudligini tekshirish
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                    if cur.fetchone():
                        logger.warning(f"'{db_name}' database allaqachon mavjud")
                        return False
                    
                    # Owner tekshirish
                    if owner:
                        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (owner,))
                        if not cur.fetchone():
                            logger.error(f"User '{owner}' mavjud emas")
                            return False
                    
                    # SQL so'rovni tuzish
                    sql = f"CREATE DATABASE {db_name} ENCODING '{encoding}' TEMPLATE {template}"
                    if owner:
                        sql += f" OWNER {owner}"
                    if tablespace:
                        sql += f" TABLESPACE {tablespace}"
                    
                    cur.execute(sql)
                    
                    # Ruxsatlarni sozlash
                    if owner:
                        cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {owner}")
                    
                    logger.success(f"Database '{db_name}' muvaffaqiyatli yaratildi")
                    
                    # Ma'lumotlarni yangilash
                    self.refresh_databases()
                    return True
                    
        except Exception as e:
            logger.error(f"Database yaratishda xato: {e}")
            return False
    
    def drop_database(self, db_name: str, force: bool = False) -> bool:
        """
        Databazani o'chirish
        
        Args:
            db_name: Database nomi
            force: Majburiy o'chirish
        """
        try:
            if db_name in ['postgres', 'template0', 'template1']:
                logger.error("System databazalarini o'chirish mumkin emas")
                return False
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Aktiv ulanishlarni uzish
                    if force:
                        cur.execute("""
                            SELECT pg_terminate_backend(pid)
                            FROM pg_stat_activity
                            WHERE datname = %s AND pid != pg_backend_pid()
                        """, (db_name,))
                    
                    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
                    logger.success(f"Database '{db_name}' o'chirildi")
                    
                    self.refresh_databases()
                    return True
                    
        except Exception as e:
            logger.error(f"Database o'chirishda xato: {e}")
            return False
    
    # ========================================================================
    # USER BOSHQARISH
    # ========================================================================
    
    def create_user(self, username: str, password: str = None, 
                   superuser: bool = False, createdb: bool = False, 
                   createrole: bool = False, login: bool = True,
                   connection_limit: int = -1, valid_until: str = None) -> bool:
        """
        Yangi user yaratish
        
        Args:
            username: User nomi
            password: Parol
            superuser: Superuser huquqi
            createdb: Database yaratish huquqi
            createrole: Rol yaratish huquqi
            login: Login qilish huquqi
            connection_limit: Ulanishlar limiti
            valid_until: Amal qilish muddati
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # User mavjudligini tekshirish
                    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (username,))
                    if cur.fetchone():
                        logger.warning(f"User '{username}' allaqachon mavjud")
                        return False
                    
                    # Kuchli parol generatsiya qilish
                    if not password:
                        password = self._generate_strong_password()
                        logger.info(f"Avtomatik parol generatsiya qilindi: {password}")
                    
                    # Parolni tekshirish
                    if not self._validate_password(password):
                        logger.error("Parol yetarlicha kuchli emas")
                        return False
                    
                    # SQL so'rovni tuzish
                    sql = f"CREATE USER {username} WITH"
                    options = []
                    
                    if superuser:
                        options.append("SUPERUSER")
                    else:
                        options.append("NOSUPERUSER")
                    
                    if createdb:
                        options.append("CREATEDB")
                    
                    if createrole:
                        options.append("CREATEROLE")
                    
                    if not login:
                        options.append("NOLOGIN")
                    
                    if connection_limit > 0:
                        options.append(f"CONNECTION LIMIT {connection_limit}")
                    
                    if valid_until:
                        options.append(f"VALID UNTIL '{valid_until}'")
                    
                    options.append(f"PASSWORD '{password}'")
                    
                    sql += " " + " ".join(options)
                    cur.execute(sql)
                    
                    logger.success(f"User '{username}' muvaffaqiyatli yaratildi")
                    
                    # User ma'lumotlarini saqlash
                    self.refresh_users()
                    return True
                    
        except Exception as e:
            logger.error(f"User yaratishda xato: {e}")
            return False
    
    def drop_user(self, username: str, reassign_to: str = None) -> bool:
        """
        Userni o'chirish
        
        Args:
            username: User nomi
            reassign_to: Objectlarni boshqa userga biriktirish
        """
        try:
            if username == 'postgres':
                logger.error("Postgres superuserni o'chirish mumkin emas")
                return False
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Objectlarni boshqa userga biriktirish
                    if reassign_to:
                        cur.execute(f"REASSIGN OWNED BY {username} TO {reassign_to}")
                        cur.execute(f"DROP OWNED BY {username}")
                    
                    cur.execute(f"DROP USER IF EXISTS {username}")
                    logger.success(f"User '{username}' o'chirildi")
                    
                    self.refresh_users()
                    return True
                    
        except Exception as e:
            logger.error(f"User o'chirishda xato: {e}")
            return False
    
    def modify_user(self, username: str, **kwargs) -> bool:
        """
        Userni sozlamalarini o'zgartirish
        
        Args:
            username: User nomi
            **kwargs: O'zgartiriladigan parametrlar
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    options = []
                    
                    for key, value in kwargs.items():
                        if key == 'password':
                            options.append(f"PASSWORD '{value}'")
                        elif key == 'superuser':
                            options.append("SUPERUSER" if value else "NOSUPERUSER")
                        elif key == 'createdb':
                            options.append("CREATEDB" if value else "NOCREATEDB")
                        elif key == 'createrole':
                            options.append("CREATEROLE" if value else "NOCREATEROLE")
                        elif key == 'login':
                            options.append("LOGIN" if value else "NOLOGIN")
                        elif key == 'connection_limit':
                            options.append(f"CONNECTION LIMIT {value}")
                        elif key == 'valid_until':
                            options.append(f"VALID UNTIL '{value}'")
                        elif key == 'rename':
                            cur.execute(f"ALTER USER {username} RENAME TO {value}")
                            username = value
                    
                    if options:
                        cur.execute(f"ALTER USER {username} WITH " + " ".join(options))
                    
                    logger.success(f"User '{username}' yangilandi")
                    self.refresh_users()
                    return True
                    
        except Exception as e:
            logger.error(f"Userni o'zgartirishda xato: {e}")
            return False
    
    # ========================================================================
    # RUXSATLAR BOSHQARISH
    # ========================================================================
    
    def grant_privileges(self, username: str, db_name: str = None, 
                        schema: str = 'public', object_type: str = 'ALL',
                        privileges: List[str] = None) -> bool:
        """
        Userga ruxsatlar berish
        
        Args:
            username: User nomi
            db_name: Database nomi
            schema: Schema nomi
            object_type: Object turi (TABLE, SEQUENCE, FUNCTION, ALL)
            privileges: Ruxsatlar ro'yxati
        """
        try:
            with self.get_connection(db_name=db_name) as conn:
                with conn.cursor() as cur:
                    
                    if object_type.upper() == 'ALL':
                        object_types = ['TABLES', 'SEQUENCES', 'FUNCTIONS']
                    else:
                        object_types = [object_type]
                    
                    default_privileges = privileges or ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
                    priv_str = ','.join(default_privileges)
                    
                    for obj_type in object_types:
                        # Default ruxsatlar
                        cur.execute(f"""
                            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema}
                            GRANT {priv_str} ON {obj_type} TO {username}
                        """)
                        
                        # Mavjud objectlar uchun ruxsatlar
                        cur.execute(f"""
                            GRANT {priv_str} ON ALL {obj_type} IN SCHEMA {schema} TO {username}
                        """)
                    
                    # Database ulanish ruxsati
                    if db_name:
                        cur.execute(f"GRANT CONNECT ON DATABASE {db_name} TO {username}")
                    
                    logger.success(f"Ruxsatlar '{username}' ga berildi")
                    return True
                    
        except Exception as e:
            logger.error(f"Ruxsat berishda xato: {e}")
            return False
    
    def revoke_privileges(self, username: str, db_name: str = None,
                         schema: str = 'public', object_type: str = 'ALL',
                         privileges: List[str] = None) -> bool:
        """
        Userdan ruxsatlarni olib tashlash
        """
        try:
            with self.get_connection(db_name=db_name) as conn:
                with conn.cursor() as cur:
                    
                    if object_type.upper() == 'ALL':
                        object_types = ['TABLES', 'SEQUENCES', 'FUNCTIONS']
                    else:
                        object_types = [object_type]
                    
                    priv_str = ','.join(privileges) if privileges else 'ALL'
                    
                    for obj_type in object_types:
                        cur.execute(f"""
                            REVOKE {priv_str} ON ALL {obj_type} IN SCHEMA {schema} FROM {username}
                        """)
                    
                    logger.success(f"Ruxsatlar '{username}' dan olib tashlandi")
                    return True
                    
        except Exception as e:
            logger.error(f"Ruxsatlarni olib tashlashda xato: {e}")
            return False
    
    # ========================================================================
    # ROLLAR BOSHQARISH
    # ========================================================================
    
    def create_role(self, role_name: str, parent_role: str = None) -> bool:
        """Yangi rol yaratish"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"CREATE ROLE {role_name}")
                    
                    if parent_role:
                        cur.execute(f"GRANT {parent_role} TO {role_name}")
                    
                    logger.success(f"Rol '{role_name}' yaratildi")
                    return True
                    
        except Exception as e:
            logger.error(f"Rol yaratishda xato: {e}")
            return False
    
    def assign_role(self, username: str, role_name: str) -> bool:
        """Userga rol biriktirish"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"GRANT {role_name} TO {username}")
                    logger.success(f"Rol '{role_name}' user '{username}' ga biriktirildi")
                    return True
                    
        except Exception as e:
            logger.error(f"Rol biriktirishda xato: {e}")
            return False
    
    # ========================================================================
    # MONITORING VA KUZATISH
    # ========================================================================
    
    def start_monitoring(self):
        """Monitoring tizimini ishga tushurish"""
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.success("Monitoring tizimi ishga tushdi")
    
    def stop_monitoring(self):
        """Monitoring tizimini to'xtatish"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("Monitoring tizimi to'xtatildi")
    
    def _monitor_loop(self):
        """Monitoring sikli"""
        while self.monitoring_active:
            try:
                self._collect_metrics()
                time.sleep(config.MONITOR_INTERVAL)
            except Exception as e:
                logger.error(f"Monitoring xatosi: {e}")
    
    def _collect_metrics(self):
        """Metriklarni yig'ish"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                
                # Aktiv ulanishlar
                cur.execute("""
                    SELECT count(*) as total,
                           count(*) FILTER (WHERE state = 'active') as active,
                           count(*) FILTER (WHERE state = 'idle') as idle,
                           count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
                    FROM pg_stat_activity
                """)
                connections = cur.fetchone()
                
                # Database o'lchamlari
                cur.execute("""
                    SELECT datname, pg_database_size(datname) as size_bytes
                    FROM pg_database
                    ORDER BY size_bytes DESC
                """)
                db_sizes = cur.fetchall()
                
                # Sekin querylar
                cur.execute("""
                    SELECT pid, usename, query, state, 
                           age(now(), query_start) as duration
                    FROM pg_stat_activity
                    WHERE state = 'active' 
                      AND query NOT LIKE '%pg_stat_activity%'
                      AND age(now(), query_start) > interval '5 seconds'
                    ORDER BY duration DESC
                """)
                slow_queries = cur.fetchall()
                
                # Ma'lumotlarni saqlash
                self.metrics = {
                    'timestamp': datetime.datetime.now(),
                    'connections': connections,
                    'database_sizes': db_sizes,
                    'slow_queries': slow_queries
                }
    
    def get_database_stats(self, db_name: str = None) -> Dict:
        """Database statistikasini olish"""
        stats = {}
        
        with self.get_connection(db_name=db_name) as conn:
            with conn.cursor() as cur:
                
                # Umumiy statistika
                cur.execute("""
                    SELECT 
                        sum(pg_database_size(datname)) as total_size,
                        count(*) as database_count
                    FROM pg_database
                    WHERE datistemplate = false
                """)
                stats['overall'] = cur.fetchone()
                
                # Har bir database haqida ma'lumot
                cur.execute("""
                    SELECT 
                        d.datname,
                        pg_size_pretty(pg_database_size(d.datname)) as size,
                        pg_database_size(d.datname) as size_bytes,
                        u.usename as owner,
                        d.datconnlimit as conn_limit,
                        (SELECT count(*) FROM pg_stat_activity WHERE datname = d.datname) as connections
                    FROM pg_database d
                    JOIN pg_user u ON d.datdba = u.usesysid
                    WHERE d.datistemplate = false
                    ORDER BY pg_database_size(d.datname) DESC
                """)
                stats['databases'] = cur.fetchall()
                
                # Cache hit ratio
                cur.execute("""
                    SELECT 
                        sum(heap_blks_hit) as heap_hits,
                        sum(heap_blks_read) as heap_reads,
                        sum(idx_blks_hit) as idx_hits,
                        sum(idx_blks_read) as idx_reads
                    FROM pg_statio_user_tables
                """)
                hits = cur.fetchone()
                
                if hits:
                    total_heap = hits[0] + hits[1]
                    total_idx = hits[2] + hits[3]
                    stats['cache_hit_ratio'] = {
                        'heap': (hits[0] / total_heap * 100) if total_heap > 0 else 0,
                        'index': (hits[2] / total_idx * 100) if total_idx > 0 else 0
                    }
                
        return stats
    
    def get_user_privileges(self, username: str) -> List[Dict]:
        """User ruxsatlarini olish"""
        privileges = []
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                
                # Database ruxsatlari
                cur.execute("""
                    SELECT 
                        datname,
                        has_database_privilege(%s, datname, 'CONNECT') as can_connect,
                        has_database_privilege(%s, datname, 'CREATE') as can_create,
                        has_database_privilege(%s, datname, 'TEMPORARY') as can_temp
                    FROM pg_database
                    WHERE datistemplate = false
                """, (username, username, username))
                
                for row in cur.fetchall():
                    privileges.append({
                        'type': 'database',
                        'name': row[0],
                        'privileges': {
                            'connect': row[1],
                            'create': row[2],
                            'temporary': row[3]
                        }
                    })
                
                # Table ruxsatlari
                cur.execute("""
                    SELECT 
                        schemaname,
                        tablename,
                        has_table_privilege(%s, schemaname||'.'||tablename, 'SELECT') as can_select,
                        has_table_privilege(%s, schemaname||'.'||tablename, 'INSERT') as can_insert,
                        has_table_privilege(%s, schemaname||'.'||tablename, 'UPDATE') as can_update,
                        has_table_privilege(%s, schemaname||'.'||tablename, 'DELETE') as can_delete,
                        has_table_privilege(%s, schemaname||'.'||tablename, 'TRUNCATE') as can_truncate
                    FROM pg_tables
                    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                    LIMIT 50
                """, (username, username, username, username, username))
                
                for row in cur.fetchall():
                    privileges.append({
                        'type': 'table',
                        'schema': row[0],
                        'name': row[1],
                        'privileges': {
                            'select': row[2],
                            'insert': row[3],
                            'update': row[4],
                            'delete': row[5],
                            'truncate': row[6]
                        }
                    })
                
        return privileges
    
    # ========================================================================
    # YORDAMCHI FUNKTSIYALAR
    # ========================================================================
    
    def _generate_strong_password(self, length: int = 16) -> str:
        """Kuchli parol generatsiya qilish"""
        import secrets
        import string
        
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        password = ''.join(secrets.choice(alphabet) for _ in range(length))
        
        # Parolni tekshirish
        while not self._validate_password(password):
            password = ''.join(secrets.choice(alphabet) for _ in range(length))
        
        return password
    
    def _validate_password(self, password: str) -> bool:
        """Parol kuchliligini tekshirish"""
        if len(password) < 12:
            return False
        if not re.search(r"[A-Z]", password):
            return False
        if not re.search(r"[a-z]", password):
            return False
        if not re.search(r"\d", password):
            return False
        if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", password):
            return False
        return True
    
    def refresh_databases(self):
        """Database ro'yxatini yangilash"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT datname, pg_get_userbyid(datdba) as owner
                    FROM pg_database
                    WHERE datistemplate = false
                """)
                self.databases = {row[0]: {'owner': row[1]} for row in cur.fetchall()}
    
    def refresh_users(self):
        """User ro'yxatini yangilash"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT rolname, rolsuper, rolcreatedb, rolcreaterole,
                           rolcanlogin, rolconnlimit
                    FROM pg_roles
                    WHERE rolname NOT LIKE 'pg_%'
                    ORDER BY rolname
                """)
                self.users = {}
                for row in cur.fetchall():
                    self.users[row[0]] = {
                        'superuser': row[1],
                        'createdb': row[2],
                        'createrole': row[3],
                        'login': row[4],
                        'connlimit': row[5]
                    }

# ============================================================================
# INTERFEYS (UI)
# ============================================================================

class PostgreSQLInterface:
    """Foydalanuvchi interfeysi"""
    
    def __init__(self):
        self.pg = PostgreSQLManager()
        self.running = True
        
    def clear_screen(self):
        """Ekran tozalash"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def print_header(self):
        """Header chiqarish"""
        self.clear_screen()
        print(Fore.CYAN + "=" * 80)
        print(Fore.GREEN + """
    ╔═══════════════════════════════════════════════════════════════════╗
    ║                                                                   ║
    ║     ██████╗  ██████╗ ███████╗████████╗ ██████╗ ██████╗ ███████╗  ║
    ║     ██╔══██╗██╔═══██╗██╔════╝╚══██╔══╝██╔════╝ ██╔══██╗██╔════╝  ║
    ║     ██████╔╝██║   ██║███████╗   ██║   ██║  ███╗██████╔╝█████╗    ║
    ║     ██╔═══╝ ██║   ██║╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝    ║
    ║     ██║     ╚██████╔╝███████║   ██║   ╚██████╔╝██║  ██║███████╗  ║
    ║     ╚═╝      ╚═════╝ ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝  ║
    ║                                                                   ║
    ║                 ENTERPRISE MONITORING VA BOSHQARUV                ║
    ║                          Version 3.0.0                            ║
    ║                                                                   ║
    ╚═══════════════════════════════════════════════════════════════════╝
        """ + Style.RESET_ALL)
        print(Fore.CYAN + "=" * 80 + Style.RESET_ALL)
        
        # Status panel
        print(Fore.YELLOW + "\n📊 SERVER STATUS:" + Style.RESET_ALL)
        print(f"   • PostgreSQL: {Fore.GREEN if self._check_postgres() else Fore.RED}{'Active' if self._check_postgres() else 'Inactive'}{Style.RESET_ALL}")
        print(f"   • Monitoring: {Fore.GREEN if self.pg.monitoring_active else Fore.RED}{'Active' if self.pg.monitoring_active else 'Inactive'}{Style.RESET_ALL}")
        print(f"   • Databases: {Fore.CYAN}{len(self.pg.databases)}{Style.RESET_ALL}")
        print(f"   • Users: {Fore.CYAN}{len(self.pg.users)}{Style.RESET_ALL}")
        print(Fore.CYAN + "-" * 80 + Style.RESET_ALL)
    
    def _check_postgres(self):
        """PostgreSQL holatini tekshirish"""
        try:
            subprocess.run(['pg_isready'], check=True, capture_output=True)
            return True
        except:
            return False
    
    def print_menu(self):
        """Asosiy menyu"""
        print(Fore.CYAN + """
╔══════════════════════════════════════════════════════════════════════╗
║                        ASOSIY MENYU                                  ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  📁 DATABASE BOSHQARISH              👥 USER BOSHQARISH              ║
║  ────────────────────────            ────────────────────────        ║
║   1. Yangi database yaratish         10. Yangi user yaratish        ║
║   2. Databazani o'chirish           11. Userni o'chirish            ║
║   3. Databazalar ro'yxati          12. Userlarni ro'yxati          ║
║   4. Database hajmini ko'rish      13. Userni tahrirlash           ║
║   5. Database backup              14. User parolini o'zgartirish   ║
║   6. Databazani restore          15. User aktivligini tekshirish  ║
║                                                                      ║
║  🔐 RUXSATLAR                       📊 MONITORING                   ║
║  ────────────────────────            ────────────────────────        ║
║   7. Ruxsatlar berish              16. Monitoringni boshlash       ║
║   8. Ruxsatlarni olib tashlash     17. Monitoringni to'xtatish    ║
║   9. User ruxsatlarini ko'rish     18. Holat statistikasi         ║
║                                   19. Aktiv ulanishlar             ║
║                                   20. Sekin querylar              ║
║                                   21. Database o'lchamlari        ║
║                                                                      ║
║  🔧 SOZLAMALAR                      ℹ️  BOSHQALAR                   ║
║  ────────────────────────            ────────────────────────        ║
║  22. Rol yaratish                 25. Loglarni ko'rish            ║
║  23. Rol biriktirish             26. Konfiguratsiya              ║
║  24. Privilege larni ko'rish     27. Yordam                      ║
║                                  28. Chiqish                     ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
""" + Style.RESET_ALL)
    
    def run(self):
        """Asosiy sikl"""
        try:
            # Ma'lumotlarni yuklash
            self.pg.refresh_databases()
            self.pg.refresh_users()
            
            while self.running:
                self.print_header()
                self.print_menu()
                
                choice = input(Fore.YELLOW + "🔷 Tanlang [1-28]: " + Style.RESET_ALL)
                
                if choice == '1':
                    self._create_database_ui()
                elif choice == '2':
                    self._drop_database_ui()
                elif choice == '3':
                    self._list_databases_ui()
                elif choice == '4':
                    self._database_size_ui()
                elif choice == '5':
                    self._backup_database_ui()
                elif choice == '6':
                    self._restore_database_ui()
                elif choice == '7':
                    self._grant_privileges_ui()
                elif choice == '8':
                    self._revoke_privileges_ui()
                elif choice == '9':
                    self._show_user_privileges_ui()
                elif choice == '10':
                    self._create_user_ui()
                elif choice == '11':
                    self._drop_user_ui()
                elif choice == '12':
                    self._list_users_ui()
                elif choice == '13':
                    self._modify_user_ui()
                elif choice == '14':
                    self._change_password_ui()
                elif choice == '15':
                    self._check_user_activity_ui()
                elif choice == '16':
                    self.pg.start_monitoring()
                elif choice == '17':
                    self.pg.stop_monitoring()
                elif choice == '18':
                    self._show_stats_ui()
                elif choice == '19':
                    self._show_connections_ui()
                elif choice == '20':
                    self._show_slow_queries_ui()
                elif choice == '21':
                    self._show_database_sizes_ui()
                elif choice == '22':
                    self._create_role_ui()
                elif choice == '23':
                    self._assign_role_ui()
                elif choice == '24':
                    self._show_privileges_ui()
                elif choice == '25':
                    self._show_logs_ui()
                elif choice == '26':
                    self._configure_ui()
                elif choice == '27':
                    self._show_help_ui()
                elif choice == '28':
                    self._exit_ui()
                else:
                    print(Fore.RED + "❌ Noto'g'ri tanlov!" + Style.RESET_ALL)
                    time.sleep(1)
        
        except KeyboardInterrupt:
            self._exit_ui()
    
    # ========================================================================
    # UI FUNKTSIYALAR
    # ========================================================================
    
    def _create_database_ui(self):
        """Database yaratish interfeysi"""
        self.clear_screen()
        print(Fore.CYAN + "\n📁 YANGI DATABASE YARATISH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        db_name = input("Database nomi: ").strip()
        owner = input("Egasi (default: postgres): ").strip() or "postgres"
        encoding = input("Kodirovka (default: UTF8): ").strip() or "UTF8"
        
        if self.pg.create_database(db_name, owner, encoding):
            print(Fore.GREEN + f"\n✅ Database '{db_name}' muvaffaqiyatli yaratildi!" + Style.RESET_ALL)
        else:
            print(Fore.RED + f"\n❌ Database yaratishda xato!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _create_user_ui(self):
        """User yaratish interfeysi"""
        self.clear_screen()
        print(Fore.CYAN + "\n👤 YANGI USER YARATISH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        username = input("Username: ").strip()
        
        print("\nParol (bo'sh qoldirilsa avtomatik generatsiya qilinadi):")
        use_auto = input("Avtomatik generatsiya? [y/N]: ").strip().lower()
        
        password = None
        if use_auto != 'y':
            password = getpass.getpass("Parol: ")
            confirm = getpass.getpass("Parolni tasdiqlang: ")
            if password != confirm:
                print(Fore.RED + "❌ Parollar mos kelmadi!" + Style.RESET_ALL)
                input("Davom etish uchun Enter bosing...")
                return
        
        print("\nRuxsatlar:")
        superuser = input("Superuser? [y/N]: ").strip().lower() == 'y'
        createdb = input("Database yaratish huquqi? [y/N]: ").strip().lower() == 'y'
        createrole = input("Rol yaratish huquqi? [y/N]: ").strip().lower() == 'y'
        
        conn_limit = input("Ulanishlar limiti (default: -1): ").strip()
        conn_limit = int(conn_limit) if conn_limit else -1
        
        if self.pg.create_user(username, password, superuser, createdb, 
                              createrole, True, conn_limit):
            print(Fore.GREEN + f"\n✅ User '{username}' muvaffaqiyatli yaratildi!" + Style.RESET_ALL)
            if not password:
                print(Fore.YELLOW + f"\n⚠️  Parol avtomatik generatsiya qilindi!" + Style.RESET_ALL)
        else:
            print(Fore.RED + f"\n❌ User yaratishda xato!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _grant_privileges_ui(self):
        """Ruxsat berish interfeysi"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔐 RUXSATLAR BERISH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        username = input("User nomi: ").strip()
        
        print("\nRuxsat turlari:")
        print("1. Barcha ruxsatlar (SELECT, INSERT, UPDATE, DELETE)")
        print("2. Faqat o'qish (SELECT)")
        print("3. O'qish va yozish (SELECT, INSERT, UPDATE)")
        print("4. Tanlov")
        
        choice = input("Tanlang [1-4]: ").strip()
        
        privileges = {
            '1': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            '2': ['SELECT'],
            '3': ['SELECT', 'INSERT', 'UPDATE']
        }.get(choice)
        
        if choice == '4':
            priv_input = input("Ruxsatlarni vergul bilan ajrating (SELECT,INSERT,UPDATE): ").strip()
            privileges = [p.strip().upper() for p in priv_input.split(',')]
        
        db_name = input("\nDatabase nomi (barchasi uchun Enter): ").strip() or None
        
        if self.pg.grant_privileges(username, db_name, privileges=privileges):
            print(Fore.GREEN + f"\n✅ Ruxsatlar '{username}' ga berildi!" + Style.RESET_ALL)
        else:
            print(Fore.RED + f"\n❌ Ruxsat berishda xato!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _list_databases_ui(self):
        """Database ro'yxatini ko'rsatish"""
        self.clear_screen()
        print(Fore.CYAN + "\n📊 DATABASELAR RO'YXATI" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 80 + Style.RESET_ALL)
        
        stats = self.pg.get_database_stats()
        
        table = PrettyTable()
        table.field_names = ["Database", "Owner", "Hajmi", "Ulanishlar", "Limit"]
        table.align["Database"] = "l"
        table.align["Hajmi"] = "r"
        
        for db in stats['databases']:
            table.add_row([
                db[0],
                db[3],
                db[1],
                db[5],
                db[4] if db[4] > 0 else 'unlimited'
            ])
        
        print(table)
        
        print(f"\nJami database: {len(stats['databases'])}")
        print(f"Umumiy hajm: {stats['overall'][1] if stats['overall'] else 'N/A'}")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _list_users_ui(self):
        """User ro'yxatini ko'rsatish"""
        self.clear_screen()
        print(Fore.CYAN + "\n👥 USERLAR RO'YXATI" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 80 + Style.RESET_ALL)
        
        table = PrettyTable()
        table.field_names = ["User", "Superuser", "Create DB", "Create Role", "Login", "Conn Limit"]
        
        for username, info in self.pg.users.items():
            table.add_row([
                username,
                '✓' if info['superuser'] else '✗',
                '✓' if info['createdb'] else '✗',
                '✓' if info['createrole'] else '✗',
                '✓' if info['login'] else '✗',
                info['connlimit'] if info['connlimit'] > 0 else 'unlimited'
            ])
        
        print(table)
        print(f"\nJami user: {len(self.pg.users)}")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _show_stats_ui(self):
        """Umumiy statistika"""
        self.clear_screen()
        print(Fore.CYAN + "\n📈 POSTGRESQL STATISTIKA" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        stats = self.pg.get_database_stats()
        
        print(Fore.YELLOW + "\n📊 DATABASE STATISTIKASI:" + Style.RESET_ALL)
        print(f"   Jami database: {len(stats['databases'])}")
        print(f"   Umumiy hajm: {stats['overall'][1] if stats['overall'] else 'N/A'}")
        
        if 'cache_hit_ratio' in stats:
            print(Fore.YELLOW + "\n💾 CACHE HIT RATIO:" + Style.RESET_ALL)
            print(f"   Table cache: {stats['cache_hit_ratio']['heap']:.2f}%")
            print(f"   Index cache: {stats['cache_hit_ratio']['index']:.2f}%")
        
        # Aktiv ulanishlar
        with self.pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT state, count(*) 
                    FROM pg_stat_activity 
                    GROUP BY state
                """)
                states = cur.fetchall()
                
                print(Fore.YELLOW + "\n🔌 ULANISHLAR:" + Style.RESET_ALL)
                for state, count in states:
                    print(f"   {state or 'unknown'}: {count}")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _show_connections_ui(self):
        """Aktiv ulanishlarni ko'rsatish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔌 AKTIV ULANISHLAR" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 100 + Style.RESET_ALL)
        
        with self.pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        pid,
                        usename,
                        datname,
                        client_addr,
                        state,
                        query_start,
                        age(now(), query_start) as duration,
                        query
                    FROM pg_stat_activity
                    WHERE pid != pg_backend_pid()
                    ORDER BY query_start DESC NULLS LAST
                    LIMIT 50
                """)
                
                connections = cur.fetchall()
                
                if not connections:
                    print("Aktiv ulanishlar yo'q")
                else:
                    table = PrettyTable()
                    table.field_names = ["PID", "User", "Database", "Client", "State", "Duration", "Query"]
                    table.max_width = 80
                    
                    for conn in connections:
                        table.add_row([
                            conn[0],
                            conn[1] or 'system',
                            conn[2] or 'none',
                            conn[3] or 'local',
                            conn[4] or 'idle',
                            str(conn[6]).split('.')[0] if conn[6] else '0:00:00',
                            (conn[7][:50] + '...') if conn[7] and len(conn[7]) > 50 else conn[7]
                        ])
                    
                    print(table)
                    print(f"\nJami ulanishlar: {len(connections)}")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _show_slow_queries_ui(self):
        """Sekin querylarni ko'rsatish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🐌 SEKIN QUERYLAR (>5 sekund)" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 100 + Style.RESET_ALL)
        
        with self.pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        pid,
                        usename,
                        datname,
                        query,
                        state,
                        age(now(), query_start) as duration,
                        wait_event_type || ': ' || wait_event as waiting
                    FROM pg_stat_activity
                    WHERE state = 'active' 
                      AND query NOT LIKE '%pg_stat_activity%'
                      AND age(now(), query_start) > interval '5 seconds'
                    ORDER BY duration DESC
                """)
                
                slow_queries = cur.fetchall()
                
                if not slow_queries:
                    print(Fore.GREEN + "✅ Sekin querylar topilmadi!" + Style.RESET_ALL)
                else:
                    for idx, query in enumerate(slow_queries, 1):
                        print(Fore.YELLOW + f"\n{idx}. QUERY (PID: {query[0]}):" + Style.RESET_ALL)
                        print(f"   User: {query[1]}")
                        print(f"   Database: {query[2]}")
                        print(f"   Davomiylik: {query[5]}")
                        print(f"   State: {query[4]}")
                        print(f"   Waiting: {query[6] or 'No'}")
                        print(f"   SQL: {query[3][:200]}...")
                        print(Fore.CYAN + "-" * 80 + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _show_database_sizes_ui(self):
        """Database o'lchamlarini ko'rsatish"""
        self.clear_screen()
        print(Fore.CYAN + "\n💿 DATABASE O'LCHAMLARI" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        with self.pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        d.datname,
                        pg_size_pretty(pg_database_size(d.datname)) as size,
                        pg_database_size(d.datname) as bytes,
                        (SELECT count(*) FROM pg_stat_activity WHERE datname = d.datname) as connections
                    FROM pg_database d
                    WHERE d.datistemplate = false
                    ORDER BY bytes DESC
                """)
                
                sizes = cur.fetchall()
                
                table = PrettyTable()
                table.field_names = ["Database", "Hajmi", "Ulanishlar"]
                
                for db in sizes:
                    table.add_row([db[0], db[1], db[3]])
                
                print(table)
                
                total_size = sum(db[2] for db in sizes)
                print(f"\nUmumiy hajm: {total_size / (1024**3):.2f} GB")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _show_user_privileges_ui(self):
        """User ruxsatlarini ko'rsatish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔑 USER RUXSATLARI" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        username = input("User nomi: ").strip()
        
        privileges = self.pg.get_user_privileges(username)
        
        if not privileges:
            print("Hech qanday ruxsat topilmadi")
        else:
            for priv in privileges[:20]:  # First 20
                if priv['type'] == 'database':
                    print(Fore.YELLOW + f"\n📁 Database: {priv['name']}" + Style.RESET_ALL)
                    for p, enabled in priv['privileges'].items():
                        status = Fore.GREEN + '✓' + Style.RESET_ALL if enabled else Fore.RED + '✗' + Style.RESET_ALL
                        print(f"   {p}: {status}")
                
                elif priv['type'] == 'table':
                    print(Fore.YELLOW + f"\n📊 Table: {priv['schema']}.{priv['name']}" + Style.RESET_ALL)
                    for p, enabled in priv['privileges'].items():
                        if enabled:
                            print(f"   ✓ {p}", end=' ')
                    print()
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _exit_ui(self):
        """Dasturdan chiqish"""
        if self.pg.monitoring_active:
            self.pg.stop_monitoring()
        
        print(Fore.GREEN + "\n✅ PostgreSQL Manager to'xtatildi!" + Style.RESET_ALL)
        print(Fore.CYAN + "   Xayr! 👋" + Style.RESET_ALL)
        self.running = False
    
    def _show_help_ui(self):
        """Yordam"""
        self.clear_screen()
        print(Fore.CYAN + """
╔══════════════════════════════════════════════════════════════════════╗
║                           YORDAM                                     ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  📘 TEZ QO'LLANMA:                                                   ║
║                                                                      ║
║  1. Yangi database yaratish:                                        ║
║     • Menyudan 1 ni tanlang                                         ║
║     • Database nomi va egasini kiriting                             ║
║     • Avtomatik ravishda barcha ruxsatlar sozlanadi                 ║
║                                                                      ║
║  2. Yangi user yaratish:                                            ║
║     • Menyudan 10 ni tanlang                                        ║
║     • Username va parol kiriting                                    ║
║     • Kuchli parol talab qilinadi (12+ belgi)                       ║
║                                                                      ║
║  3. Ruxsatlar berish:                                               ║
║     • Menyudan 7 ni tanlang                                         ║
║     • User va ruxsatlarni tanlang                                   ║
║     • Database yoki barcha databaselarga berish mumkin              ║
║                                                                      ║
║  4. Monitoring:                                                     ║
║     • Menyudan 16 ni tanlang - monitoringni boshlash                ║
║     • Real-time statistikalar                                       ║
║     • Sekin querylarni aniqlash                                     ║
║                                                                      ║
║  ⚠️  MUHIM:                                                          ║
║     • Superuser huquqini faqat kerak bo'lganda bering              ║
║     • Parollarni hech qachon oddiy qilib qo'ymang                   ║
║     • Muntazam ravishda backup qilib turing                        ║
║     • Monitoringni doim yoqib qo'ying                               ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
""" + Style.RESET_ALL)
        input("Davom etish uchun Enter bosing...")
    
    def _configure_ui(self):
        """Konfiguratsiya"""
        self.clear_screen()
        print(Fore.CYAN + "\n⚙️  KONFIGURATSIYA" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        print(f"1. Monitoring interval: {config.MONITOR_INTERVAL} sekund")
        print(f"2. Log fayli: {config.LOG_FILE}")
        print(f"3. Backup papkasi: {config.BACKUP_DIR}")
        print(f"4. Max ulanishlar: {config.MAX_CONNECTIONS}")
        
        choice = input("\nO'zgartirish uchun raqam kiriting (Enter - chiqish): ").strip()
        
        if choice == '1':
            new_val = input("Yangi monitoring interval (sekund): ")
            if new_val.isdigit():
                config.MONITOR_INTERVAL = int(new_val)
                print(Fore.GREEN + "✅ Yangilandi!" + Style.RESET_ALL)
        elif choice == '2':
            print("Log fayli o'zgartirilmaydi")
        elif choice == '3':
            new_val = input("Yangi backup papkasi: ")
            if new_val:
                config.BACKUP_DIR = new_val
                os.makedirs(new_val, exist_ok=True)
                print(Fore.GREEN + "✅ Yangilandi!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _backup_database_ui(self):
        """Database backup qilish"""
        self.clear_screen()
        print(Fore.CYAN + "\n💾 DATABASE BACKUP" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        db_name = input("Backup qilinadigan database nomi: ").strip()
        
        backup_file = f"{config.BACKUP_DIR}/{db_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        
        try:
            subprocess.run([
                'pg_dump',
                '-h', self.pg.host,
                '-p', str(self.pg.port),
                '-U', self.pg.superuser,
                '-F', 'c',
                '-f', f"{backup_file}.dump",
                db_name
            ], check=True, env={'PGPASSWORD': self.pg.superuser_password})
            
            print(Fore.GREEN + f"\n✅ Database '{db_name}' backup qilindi!" + Style.RESET_ALL)
            print(f"   Fayl: {backup_file}.dump")
            
        except Exception as e:
            print(Fore.RED + f"\n❌ Backup xatosi: {e}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _restore_database_ui(self):
        """Databazani restore qilish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔄 DATABASE RESTORE" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        db_name = input("Restore qilinadigan database nomi: ").strip()
        backup_file = input("Backup fayl yo'li: ").strip()
        
        try:
            # Aktiv ulanishlarni uzish
            with self.pg.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}'")
                    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
                    cur.execute(f"CREATE DATABASE {db_name}")
            
            # Restore
            subprocess.run([
                'pg_restore',
                '-h', self.pg.host,
                '-p', str(self.pg.port),
                '-U', self.pg.superuser,
                '-d', db_name,
                backup_file
            ], check=True, env={'PGPASSWORD': self.pg.superuser_password})
            
            print(Fore.GREEN + f"\n✅ Database '{db_name}' restore qilindi!" + Style.RESET_ALL)
            
        except Exception as e:
            print(Fore.RED + f"\n❌ Restore xatosi: {e}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _drop_database_ui(self):
        """Databazani o'chirish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🗑️  DATABASENI O'CHIRISH" + Style.RESET_ALL)
        print(Fore.RED + "⚠️  DIQQAT: Bu amalni ortga qaytarib bo'lmaydi!" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        db_name = input("O'chiriladigan database nomi: ").strip()
        
        if db_name in ['postgres', 'template0', 'template1']:
            print(Fore.RED + "❌ System databazalarini o'chirish mumkin emas!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        confirm = input(f"'{db_name}' databaseni o'chirishni tasdiqlaysizmi? [y/N]: ").strip().lower()
        
        if confirm == 'y':
            if self.pg.drop_database(db_name, force=True):
                print(Fore.GREEN + f"\n✅ Database '{db_name}' o'chirildi!" + Style.RESET_ALL)
            else:
                print(Fore.RED + f"\n❌ Database o'chirishda xato!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _drop_user_ui(self):
        """Userni o'chirish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🗑️  USERNI O'CHIRISH" + Style.RESET_ALL)
        print(Fore.RED + "⚠️  DIQQAT: Bu amalni ortga qaytarib bo'lmaydi!" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        username = input("O'chiriladigan user nomi: ").strip()
        
        if username == 'postgres':
            print(Fore.RED + "❌ Postgres superuserni o'chirish mumkin emas!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        print("\nObjectlarni boshqa userga biriktirish:")
        reassign = input("Boshqa user nomi (Enter - o'tkazib yuborish): ").strip()
        
        confirm = input(f"'{username}' userni o'chirishni tasdiqlaysizmi? [y/N]: ").strip().lower()
        
        if confirm == 'y':
            if self.pg.drop_user(username, reassign if reassign else None):
                print(Fore.GREEN + f"\n✅ User '{username}' o'chirildi!" + Style.RESET_ALL)
            else:
                print(Fore.RED + f"\n❌ Userni o'chirishda xato!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _modify_user_ui(self):
        """Userni tahrirlash"""
        self.clear_screen()
        print(Fore.CYAN + "\n✏️  USERNI TAHRIRLASH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        username = input("Tahrirlanadigan user nomi: ").strip()
        
        print("\nQaysi parametrni o'zgartirasiz?")
        print("1. Superuser huquqi")
        print("2. Database yaratish huquqi")
        print("3. Rol yaratish huquqi")
        print("4. Login huquqi")
        print("5. Ulanishlar limiti")
        print("6. User nomini o'zgartirish")
        
        choice = input("Tanlang [1-6]: ").strip()
        
        if choice == '1':
            value = input("Superuser huquqi berilsinmi? [y/N]: ").strip().lower() == 'y'
            self.pg.modify_user(username, superuser=value)
        elif choice == '2':
            value = input("Database yaratish huquqi berilsinmi? [y/N]: ").strip().lower() == 'y'
            self.pg.modify_user(username, createdb=value)
        elif choice == '3':
            value = input("Rol yaratish huquqi berilsinmi? [y/N]: ").strip().lower() == 'y'
            self.pg.modify_user(username, createrole=value)
        elif choice == '4':
            value = input("Login huquqi berilsinmi? [y/N]: ").strip().lower() == 'y'
            self.pg.modify_user(username, login=value)
        elif choice == '5':
            value = int(input("Ulanishlar limiti: ").strip())
            self.pg.modify_user(username, connection_limit=value)
        elif choice == '6':
            new_name = input("Yangi user nomi: ").strip()
            self.pg.modify_user(username, rename=new_name)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _change_password_ui(self):
        """User parolini o'zgartirish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔐 USER PAROLINI O'ZGARTIRISH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        username = input("User nomi: ").strip()
        
        print("\nYangi parol:")
        password = getpass.getpass("Parol: ")
        confirm = getpass.getpass("Parolni tasdiqlang: ")
        
        if password != confirm:
            print(Fore.RED + "❌ Parollar mos kelmadi!" + Style.RESET_ALL)
        elif not self.pg._validate_password(password):
            print(Fore.RED + "❌ Parol yetarlicha kuchli emas!" + Style.RESET_ALL)
            print("   Parol kamida 12 belgi, katta/kichik harf, son va maxsus belgi bo'lishi kerak")
        else:
            self.pg.modify_user(username, password=password)
            print(Fore.GREEN + f"\n✅ User '{username}' paroli o'zgartirildi!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _check_user_activity_ui(self):
        """User aktivligini tekshirish"""
        self.clear_screen()
        print(Fore.CYAN + "\n📊 USER AKTIVLIGI" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        username = input("User nomi: ").strip()
        
        with self.pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        datname,
                        count(*) as connections,
                        max(query_start) as last_query,
                        array_agg(DISTINCT state) as states
                    FROM pg_stat_activity
                    WHERE usename = %s
                    GROUP BY datname
                """, (username,))
                
                activities = cur.fetchall()
                
                if not activities:
                    print(f"User '{username}' aktiv emas")
                else:
                    for activity in activities:
                        print(Fore.YELLOW + f"\nDatabase: {activity[0]}" + Style.RESET_ALL)
                        print(f"   Ulanishlar: {activity[1]}")
                        print(f"   Oxirgi query: {activity[2] or 'N/A'}")
                        print(f"   Holatlar: {', '.join(activity[3] if activity[3] else [])}")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _database_size_ui(self):
        """Database hajmini ko'rish"""
        self.clear_screen()
        print(Fore.CYAN + "\n📊 DATABASE HAJMI" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        db_name = input("Database nomi (Enter - barchasi): ").strip()
        
        with self.pg.get_connection() as conn:
            with conn.cursor() as cur:
                if db_name:
                    cur.execute("SELECT pg_size_pretty(pg_database_size(%s))", (db_name,))
                    size = cur.fetchone()[0]
                    print(f"\nDatabase '{db_name}' hajmi: {Fore.GREEN}{size}{Style.RESET_ALL}")
                else:
                    cur.execute("""
                        SELECT 
                            datname,
                            pg_size_pretty(pg_database_size(datname)) as size
                        FROM pg_database
                        WHERE datistemplate = false
                        ORDER BY pg_database_size(datname) DESC
                    """)
                    sizes = cur.fetchall()
                    
                    table = PrettyTable()
                    table.field_names = ["Database", "Hajmi"]
                    
                    for db, size in sizes:
                        table.add_row([db, size])
                    
                    print(table)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _revoke_privileges_ui(self):
        """Ruxsatlarni olib tashlash"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔒 RUXSATLARNI OLIB TASHLASH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        username = input("User nomi: ").strip()
        db_name = input("Database nomi (barchasi uchun Enter): ").strip() or None
        
        privileges = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
        
        if self.pg.revoke_privileges(username, db_name, privileges=privileges):
            print(Fore.GREEN + f"\n✅ Ruxsatlar '{username}' dan olib tashlandi!" + Style.RESET_ALL)
        else:
            print(Fore.RED + f"\n❌ Ruxsatlarni olib tashlashda xato!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _create_role_ui(self):
        """Rol yaratish"""
        self.clear_screen()
        print(Fore.CYAN + "\n👥 ROL YARATISH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        role_name = input("Rol nomi: ").strip()
        parent_role = input("Asos rol (Enter - yo'q): ").strip() or None
        
        if self.pg.create_role(role_name, parent_role):
            print(Fore.GREEN + f"\n✅ Rol '{role_name}' yaratildi!" + Style.RESET_ALL)
        else:
            print(Fore.RED + f"\n❌ Rol yaratishda xato!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _assign_role_ui(self):
        """Rol biriktirish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔗 ROL BIRIKTIRISH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        username = input("User nomi: ").strip()
        role_name = input("Rol nomi: ").strip()
        
        if self.pg.assign_role(username, role_name):
            print(Fore.GREEN + f"\n✅ Rol '{role_name}' user '{username}' ga biriktirildi!" + Style.RESET_ALL)
        else:
            print(Fore.RED + f"\n❌ Rol biriktirishda xato!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _show_privileges_ui(self):
        """Barcha privilege larni ko'rish"""
        self.clear_screen()
        print(Fore.CYAN + "\n📋 PRIVILEGE LAR" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        with self.pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        grantor,
                        grantee,
                        table_schema,
                        table_name,
                        privilege_type
                    FROM information_schema.table_privileges
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                    LIMIT 50
                """)
                
                privileges = cur.fetchall()
                
                table = PrettyTable()
                table.field_names = ["Grantor", "Grantee", "Schema", "Table", "Privilege"]
                
                for priv in privileges:
                    table.add_row(priv)
                
                print(table)
                print(f"\nJami: {len(privileges)} ta privilege (faqat 50 tasi ko'rsatilmoqda)")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _show_logs_ui(self):
        """Loglarni ko'rish"""
        self.clear_screen()
        print(Fore.CYAN + "\n📜 LOGLAR" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        try:
            with open(config.LOG_FILE, 'r') as f:
                lines = f.readlines()[-50:]  # Oxirgi 50 qator
                
                for line in lines:
                    if 'ERROR' in line:
                        print(Fore.RED + line.strip() + Style.RESET_ALL)
                    elif 'WARNING' in line:
                        print(Fore.YELLOW + line.strip() + Style.RESET_ALL)
                    elif 'SUCCESS' in line or '✅' in line:
                        print(Fore.GREEN + line.strip() + Style.RESET_ALL)
                    else:
                        print(line.strip())
                        
        except FileNotFoundError:
            print("Log fayli topilmadi")
        
        input("\nDavom etish uchun Enter bosing...")

# ============================================================================
# ASOSIY DASTUR
# ============================================================================

def main():
    """Dasturni ishga tushirish"""
    
    # Root huquqini tekshirish
    if os.geteuid() != 0:
        print(Fore.RED + "❌ Bu dastur root huquqi bilan ishga tushirilishi kerak!" + Style.RESET_ALL)
        print("   sudo python3 postgres_manager.py")
        sys.exit(1)
    
    try:
        # Interfeysni ishga tushirish
        ui = PostgreSQLInterface()
        ui.run()
        
    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n\n⚠️  Dastur to'xtatildi!" + Style.RESET_ALL)
    except Exception as e:
        print(Fore.RED + f"\n❌ Kutilmagan xato: {e}" + Style.RESET_ALL)
        logger.error(f"Kutilmagan xato: {e}")
    finally:
        print(Fore.CYAN + "\n👋 Xayr!" + Style.RESET_ALL)

if __name__ == "__main__":
    main()