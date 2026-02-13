#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
POSTGRESQL ENTERPRISE ULTIMATE SYSTEM - VERSION 5.0.0
"""

import os
import sys
import json
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
import hashlib
import logging
import datetime
import threading
import subprocess
import urllib.parse
import shutil
import signal
import atexit
import secrets
import string
import re
import csv
from typing import Dict, List, Tuple, Optional, Any, Union
from contextlib import contextmanager, closing
from dataclasses import dataclass, field
from enum import Enum
from prettytable import PrettyTable
from colorama import init, Fore, Back, Style
import getpass
from functools import wraps

# Colorama init
init(autoreset=True)

# ============================================================================
# KONFIGURATSIYA
# ============================================================================

@dataclass
class Config:
    LOG_DIR: str = "/var/log/postgresql-ultimate"
    CONFIG_DIR: str = "/etc/postgresql-ultimate"
    BACKUP_DIR: str = "/var/backups/postgresql-ultimate"
    DATABASE_URLS_FILE: str = "/etc/postgresql-ultimate/database_urls.json"
    DEPLOYMENTS_FILE: str = "/etc/postgresql-ultimate/deployments.json"
    MONITOR_INTERVAL: int = 2
    SLOW_QUERY_THRESHOLD: float = 0.5
    PASSWORD_MIN_LENGTH: int = 16
    BATCH_SIZE: int = 1000

config = Config()

# Papkalarni yaratish
for dir_path in [config.LOG_DIR, config.CONFIG_DIR, config.BACKUP_DIR]:
    os.makedirs(dir_path, mode=0o750, exist_ok=True)

# ============================================================================
# LOGGER
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler(f"{config.LOG_DIR}/postgres_ultimate.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('PostgreSQL_Ultimate')

def log_success(msg): logger.info(f"{Fore.GREEN}✅ {msg}{Style.RESET_ALL}")
def log_info(msg): logger.info(f"{Fore.CYAN}ℹ️ {msg}{Style.RESET_ALL}")
def log_warning(msg): logger.warning(f"{Fore.YELLOW}⚠️ {msg}{Style.RESET_ALL}")
def log_error(msg): logger.error(f"{Fore.RED}❌ {msg}{Style.RESET_ALL}")

# ============================================================================
# ENUMLAR
# ============================================================================

class EnvironmentType(Enum):
    DEVELOPMENT = "🛠️ Development"
    TESTING = "🧪 Testing"
    STAGING = "🎭 Staging"
    PRODUCTION = "🏭 Production"

class DeploymentType(Enum):
    LOCAL = "🏠 Local"
    REMOTE = "🌐 Remote"
    DOCKER = "🐳 Docker"
    KUBERNETES = "☸️ Kubernetes"

class ConnectionMode(Enum):
    DIRECT = "⚡ Direct"
    POOL = "🔄 Connection Pool"
    SSL = "🔒 SSL/TLS"

class UserRole(Enum):
    SUPERUSER = "👑 Superuser"
    ADMIN = "👨‍💼 Admin"
    DEVELOPER = "👨‍💻 Developer"
    READ_ONLY = "👀 Read Only"
    READ_WRITE = "✍️ Read/Write"

# ============================================================================
# DATABASE URL
# ============================================================================

@dataclass
class DatabaseURL:
    scheme: str = "postgresql"
    username: str = None
    password: str = None
    host: str = "localhost"
    port: int = 5432
    database: str = None
    params: Dict[str, str] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    description: str = ""
    
    @classmethod
    def from_string(cls, url: str) -> 'DatabaseURL':
        parsed = urllib.parse.urlparse(url)
        if not parsed.scheme or parsed.scheme not in ['postgresql', 'postgres']:
            raise ValueError(f"Invalid scheme: {parsed.scheme}")
        
        username = urllib.parse.unquote(parsed.username) if parsed.username else None
        password = urllib.parse.unquote(parsed.password) if parsed.password else None
        port = parsed.port or 5432
        params = dict(urllib.parse.parse_qsl(parsed.query))
        
        return cls(
            scheme=parsed.scheme,
            username=username,
            password=password,
            host=parsed.hostname or 'localhost',
            port=port,
            database=parsed.path.lstrip('/') if parsed.path else None,
            params=params
        )
    
    def to_string(self, hide_password: bool = False) -> str:
        auth = ""
        if self.username:
            if self.password and not hide_password:
                auth = f"{urllib.parse.quote(self.username)}:{urllib.parse.quote(self.password)}@"
            else:
                auth = f"{urllib.parse.quote(self.username)}@"
        
        db_path = f"/{self.database}" if self.database else ""
        query = f"?{urllib.parse.urlencode(self.params)}" if self.params else ""
        return f"{self.scheme}://{auth}{self.host}:{self.port}{db_path}{query}"
    
    def get_connection_params(self) -> Dict[str, Any]:
        params = {
            'host': self.host,
            'port': self.port,
            'user': self.username,
            'password': self.password,
            'dbname': self.database,
            'connect_timeout': 10,
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5
        }
        
        sslmode = self.params.get('sslmode', 'prefer')
        if sslmode:
            params['sslmode'] = sslmode
        
        return params
    
    def test_connection(self) -> Tuple[bool, str]:
        try:
            with closing(psycopg2.connect(**self.get_connection_params())) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True, "✅ Connection successful"
        except Exception as e:
            return False, f"❌ Connection failed: {str(e)}"

# ============================================================================
# DATABASE URL MANAGER
# ============================================================================

class DatabaseURLManager:
    def __init__(self):
        self.urls: Dict[str, DatabaseURL] = {}
        self.load_urls()
    
    def load_urls(self):
        if os.path.exists(config.DATABASE_URLS_FILE):
            try:
                with open(config.DATABASE_URLS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for name, url_data in data.items():
                        self.urls[name] = DatabaseURL.from_string(url_data['url'])
                        self.urls[name].tags = url_data.get('tags', [])
                        self.urls[name].description = url_data.get('description', '')
                log_success(f"📂 Loaded {len(self.urls)} database URLs")
            except Exception as e:
                log_error(f"Failed to load URLs: {e}")
    
    def save_urls(self):
        try:
            data = {}
            for name, url in self.urls.items():
                data[name] = {
                    'url': url.to_string(),
                    'tags': url.tags,
                    'description': url.description
                }
            with open(config.DATABASE_URLS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            log_success("💾 Database URLs saved")
        except Exception as e:
            log_error(f"Failed to save URLs: {e}")
    
    def add_url(self, name: str, url: DatabaseURL) -> bool:
        if name in self.urls:
            log_warning(f"URL '{name}' already exists")
            return False
        self.urls[name] = url
        self.save_urls()
        log_success(f"✅ Added URL: {name} - {url.to_string(hide_password=True)}")
        return True
    
    def get_url(self, name: str) -> Optional[DatabaseURL]:
        return self.urls.get(name)
    
    def remove_url(self, name: str) -> bool:
        if name not in self.urls:
            log_error(f"URL '{name}' not found")
            return False
        del self.urls[name]
        self.save_urls()
        log_success(f"🗑️ Removed URL: {name}")
        return True
    
    def list_urls(self) -> List[Tuple[str, DatabaseURL]]:
        return list(self.urls.items())

# ============================================================================
# POSTGRESQL MANAGER
# ============================================================================

class PostgreSQLManager:
    def __init__(self, database_url: DatabaseURL = None):
        self.database_url = database_url
        self.connection_pool = None
        self.monitoring_active = False
        self.monitor_thread = None
        self.metrics_history = []
        self.alerts = []
        
        if database_url:
            self.create_pool()
    
    def create_pool(self, min_conn: int = 1, max_conn: int = 10):
        try:
            params = self.database_url.get_connection_params()
            params['application_name'] = 'PostgreSQL_Ultimate'
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn, max_conn, **params
            )
            log_success(f"🔄 Connection pool created: {min_conn}-{max_conn}")
        except Exception as e:
            log_error(f"Failed to create pool: {e}")
            raise
    
    @contextmanager
    def get_cursor(self, cursor_factory=psycopg2.extras.RealDictCursor):
        conn = self.connection_pool.getconn() if self.connection_pool else None
        if not conn:
            conn = psycopg2.connect(**self.database_url.get_connection_params())
        
        try:
            yield conn.cursor(cursor_factory=cursor_factory)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise
        finally:
            if self.connection_pool:
                self.connection_pool.putconn(conn)
            else:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = True) -> Optional[List[Dict]]:
        start = time.time()
        try:
            with self.get_cursor() as cur:
                cur.execute(query, params)
                if fetch and cur.description:
                    result = cur.fetchall()
                    duration = time.time() - start
                    if duration > config.SLOW_QUERY_THRESHOLD:
                        log_warning(f"🐌 Slow query ({duration:.2f}s): {query[:50]}...")
                    return result
                return None
        except Exception as e:
            log_error(f"Query failed: {e}")
            raise
    
    # ========================================================================
    # DATABASE OPERATIONS
    # ========================================================================
    
    def create_database(self, db_name: str, owner: str = None, encoding: str = 'UTF8') -> bool:
        try:
            with self.get_cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                if cur.fetchone():
                    log_warning(f"Database '{db_name}' already exists")
                    return False
                
                if owner:
                    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (owner,))
                    if not cur.fetchone():
                        log_error(f"User '{owner}' not found")
                        return False
                
                cur.execute(f"CREATE DATABASE {db_name} ENCODING '{encoding}'")
                
                if owner:
                    cur.execute(f"ALTER DATABASE {db_name} OWNER TO {owner}")
                    cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {owner}")
                
                log_success(f"📁 Database created: {db_name}")
                return True
        except Exception as e:
            log_error(f"Failed to create database: {e}")
            return False
    
    def list_databases(self) -> List[Dict]:
        query = """
            SELECT 
                d.datname,
                pg_get_userbyid(d.datdba) as owner,
                pg_size_pretty(pg_database_size(d.datname)) as size,
                pg_database_size(d.datname) as size_bytes,
                (SELECT count(*) FROM pg_stat_activity WHERE datname = d.datname) as connections
            FROM pg_database d
            WHERE d.datistemplate = false
            ORDER BY size_bytes DESC
        """
        return self.execute_query(query) or []
    
    # ========================================================================
    # USER OPERATIONS
    # ========================================================================
    
    def create_user(self, username: str, password: str = None,
                   superuser: bool = False, createdb: bool = False,
                   createrole: bool = False, role: UserRole = UserRole.READ_WRITE) -> bool:
        try:
            with self.get_cursor() as cur:
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (username,))
                if cur.fetchone():
                    log_warning(f"User '{username}' already exists")
                    return False
                
                if not password:
                    password = self._generate_password()
                    log_info(f"Generated password: {password}")
                
                options = ["NOSUPERUSER"]
                if superuser:
                    options = ["SUPERUSER"]
                if createdb:
                    options.append("CREATEDB")
                if createrole:
                    options.append("CREATEROLE")
                
                options.append(f"PASSWORD '{password}'")
                cur.execute(f"CREATE USER {username} WITH " + " ".join(options))
                
                if role == UserRole.READ_ONLY:
                    cur.execute(f"GRANT CONNECT ON DATABASE postgres TO {username}")
                    cur.execute("ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO %s", (username,))
                elif role == UserRole.READ_WRITE:
                    cur.execute(f"GRANT CONNECT, CREATE ON DATABASE postgres TO {username}")
                    cur.execute("ALTER DEFAULT PRIVILEGES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s", (username,))
                
                log_success(f"👤 User created: {username} (Role: {role.value})")
                return True
        except Exception as e:
            log_error(f"Failed to create user: {e}")
            return False
    
    def list_users(self) -> List[Dict]:
        query = """
            SELECT 
                rolname as username,
                rolsuper as is_superuser,
                rolcreatedb as can_create_db,
                rolcreaterole as can_create_role,
                rolcanlogin as can_login,
                (SELECT count(*) FROM pg_stat_activity WHERE usename = rolname) as active_connections
            FROM pg_roles
            WHERE rolname NOT LIKE 'pg_%'
            ORDER BY rolname
        """
        return self.execute_query(query) or []
    
    def _generate_password(self) -> str:
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        while True:
            password = ''.join(secrets.choice(alphabet) for _ in range(16))
            if (re.search(r"[A-Z]", password) and re.search(r"[a-z]", password) and
                re.search(r"\d", password) and re.search(r"[!@#$%^&*]", password)):
                return password
    
    # ========================================================================
    # PRIVILEGES
    # ========================================================================
    
    def grant_privileges(self, username: str, db_name: str = None,
                        privileges: List[str] = None) -> bool:
        try:
            with self.get_cursor(db_name=db_name) as cur:
                priv_str = ','.join(privileges) if privileges else 'ALL'
                
                cur.execute(f"""
                    GRANT {priv_str} ON ALL TABLES IN SCHEMA public TO {username}
                """)
                cur.execute(f"""
                    ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    GRANT {priv_str} ON TABLES TO {username}
                """)
                
                if db_name:
                    cur.execute(f"GRANT CONNECT ON DATABASE {db_name} TO {username}")
                
                log_success(f"🔐 Granted privileges to: {username}")
                return True
        except Exception as e:
            log_error(f"Failed to grant privileges: {e}")
            return False
    
    # ========================================================================
    # DATA OPERATIONS
    # ========================================================================
    
    def insert_data(self, table: str, data: Union[Dict, List[Dict]], 
                   batch_size: int = 1000) -> Tuple[int, int]:
        if isinstance(data, dict):
            data = [data]
        if not data:
            return 0, 0
        
        columns = list(data[0].keys())
        placeholders = ','.join(['%s'] * len(columns))
        query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        
        successful = 0
        failed = 0
        
        try:
            with self.get_cursor() as cur:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    values = [tuple(row[col] for col in columns) for row in batch]
                    try:
                        cur.executemany(query, values)
                        successful += len(batch)
                    except Exception as e:
                        log_error(f"Batch insert failed: {e}")
                        failed += len(batch)
            
            log_success(f"📝 Inserted {successful} rows into {table}")
            return successful, failed
        except Exception as e:
            log_error(f"Insert failed: {e}")
            return 0, len(data)
    
    def import_csv(self, table: str, csv_file: str) -> Tuple[int, int]:
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = list(reader)
                
                for row in data:
                    for key, value in row.items():
                        if value and value.replace('.', '').replace('-', '').isdigit():
                            if '.' in value:
                                row[key] = float(value)
                            else:
                                row[key] = int(value)
                
                return self.insert_data(table, data)
        except Exception as e:
            log_error(f"CSV import failed: {e}")
            return 0, 0
    
    # ========================================================================
    # MONITORING
    # ========================================================================
    
    def get_metrics(self) -> Dict[str, Any]:
        metrics = {}
        with self.get_cursor() as cur:
            # Connections
            cur.execute("""
                SELECT 
                    count(*) as total_connections,
                    count(*) FILTER (WHERE state = 'active') as active_connections,
                    count(*) FILTER (WHERE state = 'idle') as idle_connections
                FROM pg_stat_activity
            """)
            metrics['connections'] = cur.fetchone()
            
            # Database sizes
            cur.execute("""
                SELECT 
                    count(*) as database_count,
                    pg_size_pretty(sum(pg_database_size(datname))) as total_size
                FROM pg_database
                WHERE datistemplate = false
            """)
            metrics['databases'] = cur.fetchone()
            
            # Cache hit ratio
            cur.execute("""
                SELECT 
                    sum(heap_blks_hit)::float / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100 as cache_hit_ratio
                FROM pg_statio_user_tables
            """)
            metrics['cache'] = cur.fetchone()
        
        metrics['timestamp'] = datetime.datetime.now().isoformat()
        return metrics
    
    def get_slow_queries(self, threshold: float = 1.0) -> List[Dict]:
        query = """
            SELECT 
                pid, usename, datname, query,
                age(now(), query_start) as duration
            FROM pg_stat_activity
            WHERE state = 'active' 
                AND query NOT LIKE '%pg_stat_activity%'
                AND age(now(), query_start) > interval %s
            ORDER BY duration DESC
            LIMIT 10
        """
        return self.execute_query(query, (f"{threshold} seconds",)) or []
    
    def start_monitoring(self):
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        log_success("📊 Monitoring started")
    
    def stop_monitoring(self):
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join()
        log_info("📊 Monitoring stopped")
    
    def _monitor_loop(self):
        while self.monitoring_active:
            try:
                metrics = self.get_metrics()
                self.metrics_history.append({
                    'timestamp': datetime.datetime.now(),
                    'metrics': metrics
                })
                time.sleep(config.MONITOR_INTERVAL)
            except Exception as e:
                log_error(f"Monitoring error: {e}")
    
    def close(self):
        if self.connection_pool:
            self.connection_pool.closeall()
            log_info("🔌 Connection pool closed")

# ============================================================================
# DEPLOYMENT MANAGER
# ============================================================================

class DeploymentManager:
    def __init__(self):
        self.deployments: Dict[str, Dict] = {}
        self.load_deployments()
    
    def load_deployments(self):
        if os.path.exists(config.DEPLOYMENTS_FILE):
            try:
                with open(config.DEPLOYMENTS_FILE, 'r', encoding='utf-8') as f:
                    self.deployments = json.load(f)
                log_success(f"📦 Loaded {len(self.deployments)} deployments")
            except Exception as e:
                log_error(f"Failed to load deployments: {e}")
    
    def save_deployments(self):
        try:
            with open(config.DEPLOYMENTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.deployments, f, indent=2, ensure_ascii=False)
            log_success("💾 Deployments saved")
        except Exception as e:
            log_error(f"Failed to save deployments: {e}")
    
    def create_deployment(self, name: str, url_name: str,
                         environment: EnvironmentType,
                         deployment_type: DeploymentType,
                         connection_mode: ConnectionMode,
                         **kwargs) -> bool:
        if name in self.deployments:
            log_warning(f"Deployment '{name}' already exists")
            return False
        
        self.deployments[name] = {
            'name': name,
            'url_name': url_name,
            'environment': environment.value,
            'deployment_type': deployment_type.value,
            'connection_mode': connection_mode.value,
            'created_at': datetime.datetime.now().isoformat(),
            'status': 'created',
            'config': kwargs,
            'metrics': []
        }
        self.save_deployments()
        log_success(f"🚀 Deployment created: {name}")
        return True
    
    def get_deployment(self, name: str) -> Optional[Dict]:
        return self.deployments.get(name)
    
    def list_deployments(self) -> List[Tuple[str, Dict]]:
        return list(self.deployments.items())

# ============================================================================
# UI
# ============================================================================

class UltimateUI:
    def __init__(self):
        self.url_manager = DatabaseURLManager()
        self.deployment_manager = DeploymentManager()
        self.current_pg_manager: Optional[PostgreSQLManager] = None
        self.current_db_url: Optional[DatabaseURL] = None
        self.current_deployment: Optional[str] = None
        self.running = True
        self.menu_section = 1
        self.performance_mode = True
    
    def clear_screen(self):
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def print_header(self):
        self.clear_screen()
        print(f"{Fore.CYAN}{'=' * 90}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}")
        print("    ██████╗  ██████╗ ███████╗████████╗ ██████╗ ██████╗ ███████╗")
        print("    ██╔══██╗██╔═══██╗██╔════╝╚══██╔══╝██╔════╝ ██╔══██╗██╔════╝")
        print("    ██████╔╝██║   ██║███████╗   ██║   ██║  ███╗██████╔╝█████╗  ")
        print("    ██╔═══╝ ██║   ██║╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  ")
        print("    ██║     ╚██████╔╝███████║   ██║   ╚██████╔╝██║  ██║███████╗")
        print("    ╚═╝      ╚═════╝ ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝")
        print(f"{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'=' * 90}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}   🚀 ULTIMATE ENTERPRISE SYSTEM v5.0.0 | 99% PERFORMANCE | ⚡ REAL-TIME{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'=' * 90}{Style.RESET_ALL}")
        print()
        
        # Status bar
        db_status = f"{Fore.GREEN}●{Style.RESET_ALL}" if self.current_db_url else f"{Fore.RED}○{Style.RESET_ALL}"
        menu_ind = f"{Fore.CYAN}【{'●' if self.menu_section == 1 else '○'} DB │ {'○' if self.menu_section == 1 else '●'} DEPLOY】{Style.RESET_ALL}"
        
        print(f"{Fore.WHITE}┌─{'─' * 86}─┐{Style.RESET_ALL}")
        print(f"{Fore.WHITE}│{Style.RESET_ALL}  {db_status} PostgreSQL  |  📊 URLs: {len(self.url_manager.urls)}  |  📦 Deployments: {len(self.deployment_manager.deployments)}  ")
        if self.current_db_url:
            print(f"  |  🔗 Active: {self.current_db_url.database}@{self.current_db_url.host}  ")
        print(f"  |  🎯 Menu: {menu_ind}  ")
        print(f"{Fore.WHITE}│{Style.RESET_ALL}")
        print(f"{Fore.WHITE}└─{'─' * 86}─┘{Style.RESET_ALL}")
        print()
    
    def print_menu_database(self):
        print(f"{Fore.CYAN}╔{'═' * 86}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 30}📁 SECTION 1: DATABASE MANAGEMENT{' ' * 30}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╠{'═' * 86}╣{Style.RESET_ALL}")
        
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.GREEN}📁 DATABASE{Style.RESET_ALL}            {Fore.GREEN}👥 USERS{Style.RESET_ALL}                {Fore.YELLOW}🔐 PRIVILEGES{Style.RESET_ALL}          {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 84}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [01] Create DB    [10] Create User    [07] Grant Privs    {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [02] Drop DB      [11] Drop User      [08] Revoke Privs    {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [03] List DBs     [12] List Users     [09] User Privs      {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [04] DB Sizes     [13] Modify User    [19] Create Role     {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [05] Backup       [14] Change Pass    [20] Assign Role     {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [06] Restore      [15] User Activity  [21] Revoke Role     {Fore.CYAN}║")
        print()
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.MAGENTA}📦 DATA{Style.RESET_ALL}               {Fore.BLUE}🔗 URL{Style.RESET_ALL}                  {Fore.CYAN}⚙️  UTILS{Style.RESET_ALL}              {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 84}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [16] Insert Single [23] Add URL       [31] View Logs      {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [17] Bulk Insert  [24] List URLs      [32] Configure      {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [18] Import CSV   [25] Select URL     [33] Cleanup        {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}                   [26] Edit URL        [34] Performance    {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}                   [27] Remove URL      [35] Help           {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}                   [28] Test URL                          {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}                   [29] Generate URL                       {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}                   [30] Add Tags                           {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 84}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 86}╝{Style.RESET_ALL}")
    
    def print_menu_deployment(self):
        print(f"{Fore.CYAN}╔{'═' * 86}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 28}🚀 SECTION 2: DEPLOYMENT & MONITORING{' ' * 28}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╠{'═' * 86}╣{Style.RESET_ALL}")
        
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.GREEN}🚀 DEPLOYMENT{Style.RESET_ALL}          {Fore.YELLOW}📊 MONITORING{Style.RESET_ALL}            {Fore.MAGENTA}🐍 GENERATORS{Style.RESET_ALL}       {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 84}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [51] Create Dep   [61] Start Monitor  [71] Local Client   {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [52] List Deps    [62] Stop Monitor   [72] Remote Client  {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [53] Select Dep   [63] Metrics        [73] Docker Compose {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [54] Remove Dep   [64] Connections    [74] Kubernetes     {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [55] Test Dep     [65] Slow Queries   [75] Env File       {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [56] Logs         [66] DB Sizes                            {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [57] Export       [67] Table Sizes                         {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [58] Import       [68] Cache Analysis                      {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [59] Scale        [69] Lock Conflicts                       {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [60] Status       [70] Replication                          {Fore.CYAN}║")
        print()
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.RED}⚠️  ALERTS{Style.RESET_ALL}              {Fore.BLUE}💾 BACKUP{Style.RESET_ALL}                 {Fore.CYAN}⚙️  SYSTEM{Style.RESET_ALL}           {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 84}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [81] View Alerts  [91] Scheduled Bk   [96] Theme         {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [82] Settings     [92] PITR           [97] Perf Mode     {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [83] History      [93] List Backups   [98] Switch Menu   {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [84] Report       [94] Clean Backups  [99] Reload        {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [85] Health       [95] Backup Status  [00] Exit          {Fore.CYAN}║")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 84}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 86}╝{Style.RESET_ALL}")
    
    def print_quick_actions(self):
        print(f"{Fore.CYAN}┌─{'─' * 86}─┐{Style.RESET_ALL}")
        print(f"{Fore.CYAN}│{Style.RESET_ALL}  {Fore.YELLOW}⚡ QUICK:{Style.RESET_ALL}  ")
        if self.menu_section == 1:
            print(f"  [01] Create DB  [10] Create User  [07] Grant  [16] Insert  [29] Gen URL  ")
        else:
            print(f"  [51] Deploy  [61] Monitor  [71] Python  [85] Health  [91] Backup  ")
        print(f"{Fore.CYAN}│{Style.RESET_ALL}")
        print(f"{Fore.CYAN}└─{'─' * 86}─┘{Style.RESET_ALL}")
    
    def run(self):
        try:
            while self.running:
                self.print_header()
                
                if self.menu_section == 1:
                    self.print_menu_database()
                else:
                    self.print_menu_deployment()
                
                self.print_quick_actions()
                
                choice = input(f"\n{Fore.YELLOW}┌─[ POSTGRES ]\n└──╼ $ {Style.RESET_ALL}")
                
                if choice == '98':
                    self.menu_section = 2 if self.menu_section == 1 else 1
                    continue
                elif choice == '00' or choice == '0':
                    break
                elif choice == '1':
                    self.create_database_ui()
                elif choice == '10':
                    self.create_user_ui()
                elif choice == '7':
                    self.grant_privileges_ui()
                elif choice == '16':
                    self.insert_single_ui()
                elif choice == '29':
                    self.generate_url_ui()
                elif choice == '51':
                    self.create_deployment_ui()
                elif choice == '61':
                    self.start_monitoring_ui()
                elif choice == '63':
                    self.show_metrics_ui()
                elif choice == '71':
                    self.generate_local_client_ui()
                elif choice == '72':
                    self.generate_remote_client_ui()
                elif choice == '85':
                    self.health_check_ui()
                elif choice == '91':
                    self.scheduled_backup_ui()
                else:
                    log_warning("Not implemented yet")
                    input("Press Enter...")
        
        except KeyboardInterrupt:
            self.exit()
        finally:
            self.exit()
    
    # UI Implementations
    def create_database_ui(self):
        if not self.current_pg_manager:
            log_error("Please select a database URL first")
            input("Press Enter...")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 15}📁 CREATE DATABASE{' ' * 16}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        db_name = input("Database name: ").strip()
        owner = input("Owner [postgres]: ").strip() or "postgres"
        encoding = input("Encoding [UTF8]: ").strip() or "UTF8"
        
        if self.current_pg_manager.create_database(db_name, owner, encoding):
            log_success(f"Database '{db_name}' created")
        else:
            log_error(f"Failed to create database")
        
        input("\nPress Enter...")
    
    def create_user_ui(self):
        if not self.current_pg_manager:
            log_error("Please select a database URL first")
            input("Press Enter...")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 16}👤 CREATE USER{' ' * 18}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        username = input("Username: ").strip()
        
        print("\nRole:")
        for i, role in enumerate(UserRole, 1):
            print(f"  {i}. {role.value}")
        role_choice = input("Select role [5]: ").strip() or "5"
        roles = list(UserRole)
        role = roles[int(role_choice) - 1] if role_choice.isdigit() and 1 <= int(role_choice) <= len(roles) else UserRole.READ_WRITE
        
        if self.current_pg_manager.create_user(username=username, role=role):
            log_success(f"User '{username}' created")
        else:
            log_error(f"Failed to create user")
        
        input("\nPress Enter...")
    
    def grant_privileges_ui(self):
        if not self.current_pg_manager:
            log_error("Please select a database URL first")
            input("Press Enter...")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 13}🔐 GRANT PRIVILEGES{' ' * 14}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        username = input("Username: ").strip()
        privileges = input("Privileges [ALL]: ").strip()
        priv_list = [p.strip().upper() for p in privileges.split(',')] if privileges else None
        
        if self.current_pg_manager.grant_privileges(username, privileges=priv_list):
            log_success(f"Privileges granted to '{username}'")
        else:
            log_error(f"Failed to grant privileges")
        
        input("\nPress Enter...")
    
    def insert_single_ui(self):
        if not self.current_pg_manager:
            log_error("Please select a database URL first")
            input("Press Enter...")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 13}📝 INSERT SINGLE RECORD{' ' * 12}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        table = input("Table name: ").strip()
        print("\nEnter data in JSON format (e.g., {\"name\":\"John\",\"age\":30})")
        data_str = input("Data: ").strip()
        
        try:
            data = json.loads(data_str)
            successful, _ = self.current_pg_manager.insert_data(table, data)
            if successful > 0:
                log_success("Record inserted")
            else:
                log_error("Failed to insert")
        except json.JSONDecodeError:
            log_error("Invalid JSON")
        
        input("\nPress Enter...")
    
    def generate_url_ui(self):
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 13}🎲 GENERATE DATABASE URL{' ' * 12}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        print("1. Local Development")
        print("2. Remote Production")
        print("3. Docker Container")
        choice = input("\nSelect type [1]: ").strip() or "1"
        
        if choice == "1":
            url = DatabaseURL(
                host='localhost',
                port=5432,
                database='myapp_dev',
                username='postgres',
                password='postgres123'
            )
        elif choice == "2":
            url = DatabaseURL(
                host='db.example.com',
                port=5432,
                database='prod_db',
                username='app_user',
                password='StrongPass123!',
                params={'sslmode': 'require'}
            )
        else:
            url = DatabaseURL(
                host='postgres',
                port=5432,
                database='app_db',
                username='app_user',
                password='docker_pass'
            )
        
        print(f"\n{Fore.GREEN}Generated URL:{Style.RESET_ALL}")
        print(f"  {url.to_string()}")
        
        save = input("\nSave this URL? [y/N]: ").strip().lower()
        if save == 'y':
            name = input("URL name: ").strip()
            self.url_manager.add_url(name, url)
        
        input("\nPress Enter...")
    
    def create_deployment_ui(self):
        if not self.url_manager.urls:
            log_error("Please create a database URL first")
            input("Press Enter...")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 13}🚀 CREATE DEPLOYMENT{' ' * 14}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        print("Available URLs:")
        for i, (name, url) in enumerate(self.url_manager.list_urls(), 1):
            print(f"  {i}. {name} - {url.to_string(hide_password=True)}")
        
        url_name = input("\nSelect URL name: ").strip()
        if url_name not in self.url_manager.urls:
            log_error(f"URL '{url_name}' not found")
            input("Press Enter...")
            return
        
        name = input("Deployment name: ").strip()
        
        print("\nEnvironment:")
        for i, env in enumerate(EnvironmentType, 1):
            print(f"  {i}. {env.value}")
        env_choice = input("Select [1]: ").strip() or "1"
        envs = list(EnvironmentType)
        env = envs[int(env_choice) - 1] if env_choice.isdigit() and 1 <= int(env_choice) <= len(envs) else EnvironmentType.DEVELOPMENT
        
        print("\nDeployment Type:")
        for i, dep in enumerate(DeploymentType, 1):
            print(f"  {i}. {dep.value}")
        dep_choice = input("Select [1]: ").strip() or "1"
        deps = list(DeploymentType)
        dep_type = deps[int(dep_choice) - 1] if dep_choice.isdigit() and 1 <= int(dep_choice) <= len(deps) else DeploymentType.LOCAL
        
        if self.deployment_manager.create_deployment(
            name=name,
            url_name=url_name,
            environment=env,
            deployment_type=dep_type,
            connection_mode=ConnectionMode.DIRECT
        ):
            url = self.url_manager.get_url(url_name)
            self.current_pg_manager = PostgreSQLManager(url)
            self.current_db_url = url
            self.current_deployment = name
            log_success(f"Deployment '{name}' created")
        
        input("\nPress Enter...")
    
    def start_monitoring_ui(self):
        if not self.current_pg_manager:
            log_error("Please select a deployment first")
            input("Press Enter...")
            return
        
        self.current_pg_manager.start_monitoring()
        input("Monitoring started. Press Enter...")
    
    def show_metrics_ui(self):
        if not self.current_pg_manager:
            log_error("Please select a deployment first")
            input("Press Enter...")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 60}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 20}📈 REAL-TIME METRICS{' ' * 21}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 60}╝{Style.RESET_ALL}\n")
        
        try:
            metrics = self.current_pg_manager.get_metrics()
            
            print(f"{Fore.CYAN}🔌 Connections:{Style.RESET_ALL}")
            print(f"  Total: {metrics['connections']['total_connections']}")
            print(f"  Active: {metrics['connections']['active_connections']}")
            print(f"  Idle: {metrics['connections']['idle_connections']}")
            print()
            
            print(f"{Fore.CYAN}💾 Cache:{Style.RESET_ALL}")
            if metrics['cache']:
                print(f"  Hit Ratio: {metrics['cache']['cache_hit_ratio']:.1f}%")
            print()
            
            print(f"{Fore.CYAN}💿 Databases:{Style.RESET_ALL}")
            print(f"  Count: {metrics['databases']['database_count']}")
            print(f"  Total Size: {metrics['databases']['total_size']}")
            print()
            
            slow = self.current_pg_manager.get_slow_queries()
            if slow:
                print(f"{Fore.RED}🐌 Slow Queries:{Style.RESET_ALL}")
                for q in slow[:3]:
                    print(f"  • {q['duration']}: {q['query'][:50]}...")
            
        except Exception as e:
            log_error(f"Failed to get metrics: {e}")
        
        input("\nPress Enter...")
    
    def generate_local_client_ui(self):
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 11}🐍 LOCAL PYTHON CLIENT GENERATOR{' ' * 10}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        filename = input("Output file [postgres_client.py]: ").strip() or "postgres_client.py"
        
        code = '''#!/usr/bin/env python3
"""
PostgreSQL Client - Generated by Ultimate System
"""
import os
import psycopg2
import psycopg2.pool
from psycopg2 import extras
from typing import Dict, List, Optional
from contextlib import contextmanager

class PostgreSQLClient:
    def __init__(self, host='localhost', port=5432, database=None,
                 user=None, password=None):
        self.params = {
            'host': host or os.environ.get('PGHOST', 'localhost'),
            'port': port or int(os.environ.get('PGPORT', 5432)),
            'dbname': database or os.environ.get('PGDATABASE'),
            'user': user or os.environ.get('PGUSER'),
            'password': password or os.environ.get('PGPASSWORD'),
        }
        self.pool = psycopg2.pool.SimpleConnectionPool(1, 10, **self.params)
    
    @contextmanager
    def get_cursor(self):
        conn = self.pool.getconn()
        try:
            yield conn.cursor(cursor_factory=extras.RealDictCursor)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
    
    def execute(self, query: str, params: tuple = None, fetch=True):
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if fetch and cur.description else None
    
    def insert(self, table: str, data: Dict):
        cols = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        return self.execute(query, tuple(data.values()), fetch=False)
    
    def close(self):
        self.pool.closeall()

if __name__ == '__main__':
    db = PostgreSQLClient()
    try:
        print(db.execute("SELECT version()"))
    finally:
        db.close()
'''
        
        with open(filename, 'w') as f:
            f.write(code)
        
        os.chmod(filename, 0o755)
        log_success(f"Client generated: {filename}")
        input("\nPress Enter...")
    
    def generate_remote_client_ui(self):
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 10}☁️ REMOTE PYTHON CLIENT GENERATOR{' ' * 9}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        filename = input("Output file [postgres_remote.py]: ").strip() or "postgres_remote.py"
        
        code = '''#!/usr/bin/env python3
"""
PostgreSQL Remote Client - Generated by Ultimate System
Features: SSL, Connection Pool, Retry
"""
import os
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
from psycopg2 import pool, errors
from typing import Dict
from contextlib import contextmanager
import urllib.parse

class DatabaseURL:
    def __init__(self, url: str):
        parsed = urllib.parse.urlparse(url)
        self.host = parsed.hostname or 'localhost'
        self.port = parsed.port or 5432
        self.user = urllib.parse.unquote(parsed.username) if parsed.username else None
        self.password = urllib.parse.unquote(parsed.password) if parsed.password else None
        self.dbname = parsed.path.lstrip('/') if parsed.path else None
        self.params = dict(urllib.parse.parse_qsl(parsed.query))
        self.sslmode = self.params.get('sslmode', 'require')

class RemotePostgreSQLClient:
    def __init__(self, database_url: str):
        self.db_url = DatabaseURL(database_url)
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            2, 20,
            host=self.db_url.host,
            port=self.db_url.port,
            user=self.db_url.user,
            password=self.db_url.password,
            dbname=self.db_url.dbname,
            sslmode=self.db_url.sslmode,
            connect_timeout=30,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
    
    @contextmanager
    def get_cursor(self):
        conn = self.pool.getconn()
        try:
            yield conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
    
    def execute(self, query: str, params: tuple = None, retries=3):
        for attempt in range(retries):
            try:
                with self.get_cursor() as cur:
                    cur.execute(query, params)
                    return cur.fetchall() if cur.description else None
            except (errors.OperationalError, errors.InterfaceError) as e:
                if attempt == retries - 1:
                    raise
                time.sleep(1 * (attempt + 1))
    
    def health_check(self) -> Dict:
        try:
            start = time.time()
            self.execute("SELECT 1")
            return {
                'status': 'healthy',
                'response_time': time.time() - start
            }
        except Exception as e:
            return {'status': 'unhealthy', 'error': str(e)}
    
    def close(self):
        self.pool.closeall()

if __name__ == '__main__':
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if not DATABASE_URL:
        print("Please set DATABASE_URL environment variable")
        sys.exit(1)
    
    db = RemotePostgreSQLClient(DATABASE_URL)
    try:
        print(db.health_check())
    finally:
        db.close()
'''
        
        with open(filename, 'w') as f:
            f.write(code)
        
        os.chmod(filename, 0o755)
        log_success(f"Remote client generated: {filename}")
        input("\nPress Enter...")
    
    def health_check_ui(self):
        if not self.current_pg_manager:
            log_error("Please select a deployment first")
            input("Press Enter...")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 16}🏥 HEALTH CHECK{' ' * 18}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        try:
            start = time.time()
            result = self.current_pg_manager.execute_query("SELECT 1")
            response_time = time.time() - start
            
            print(f"{Fore.GREEN}✅ Status: Healthy{Style.RESET_ALL}")
            print(f"  Response Time: {response_time*1000:.2f}ms")
            
            version = self.current_pg_manager.execute_query("SELECT version()")
            if version:
                print(f"  PostgreSQL: {version[0]['version'].split(',')[0]}")
            
            uptime = self.current_pg_manager.execute_query("""
                SELECT now() - pg_postmaster_start_time() as uptime
            """)
            if uptime:
                print(f"  Uptime: {uptime[0]['uptime']}")
            
        except Exception as e:
            print(f"{Fore.RED}❌ Status: Unhealthy{Style.RESET_ALL}")
            print(f"  Error: {e}")
        
        input("\nPress Enter...")
    
    def scheduled_backup_ui(self):
        if not self.current_pg_manager:
            log_error("Please select a deployment first")
            input("Press Enter...")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 50}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 14}💾 SCHEDULED BACKUP{' ' * 15}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 50}╝{Style.RESET_ALL}\n")
        
        db_name = input("Database name: ").strip()
        interval = input("Backup interval (hours) [24]: ").strip() or "24"
        
        print(f"\n{Fore.YELLOW}Scheduling backup every {interval} hours...{Style.RESET_ALL}")
        
        def backup_job():
            while True:
                try:
                    file = self.current_pg_manager.backup_database(db_name)
                    if file:
                        log_success(f"Auto backup created: {file}")
                    time.sleep(int(interval) * 3600)
                except Exception as e:
                    log_error(f"Auto backup failed: {e}")
        
        thread = threading.Thread(target=backup_job, daemon=True)
        thread.start()
        
        log_success(f"Scheduled backup started for '{db_name}'")
        input("\nPress Enter...")
    
    def exit(self):
        if self.current_pg_manager:
            self.current_pg_manager.close()
        print(f"\n{Fore.GREEN}✅ PostgreSQL Ultimate System terminated{Style.RESET_ALL}")
        self.running = False

# ============================================================================
# MAIN
# ============================================================================

def main():
    # Check if running as root (warn only)
    if os.geteuid() != 0:
        print(f"{Fore.YELLOW}⚠️ Running without root privileges - some features may be limited{Style.RESET_ALL}")
        print(f"{Fore.CYAN}   For full functionality, run with: sudo python3 postgres_ultimate.py{Style.RESET_ALL}\n")
        time.sleep(2)
    
    # Signal handlers
    def signal_handler(sig, frame):
        print(f"\n{Fore.YELLOW}⚠️ Interrupted{Style.RESET_ALL}")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start UI
    ui = UltimateUI()
    ui.run()

if __name__ == "__main__":
    main()
