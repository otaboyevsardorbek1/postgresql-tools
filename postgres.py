#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
═══════════════════════════════════════════════════════════════════════════════
POSTGRESQL ENTERPRISE ULTIMATE SYSTEM - VERSION 5.0.0
═══════════════════════════════════════════════════════════════════════════════

██████╗  ██████╗ ███████╗████████╗ ██████╗ ██████╗ ███████╗███████╗ ██████╗ ██╗     
██╔══██╗██╔═══██╗██╔════╝╚══██╔══╝██╔════╝ ██╔══██╗██╔════╝██╔════╝██╔═══██╗██║     
██████╔╝██║   ██║███████╗   ██║   ██║  ███╗██████╔╝█████╗  █████╗  ██║   ██║██║     
██╔═══╝ ██║   ██║╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  ██╔══╝  ██║   ██║██║     
██║     ╚██████╔╝███████║   ██║   ╚██████╔╝██║  ██║███████╗██║     ╚██████╔╝███████╗
╚═╝      ╚═════╝ ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝      ╚═════╝ ╚══════╝

═══════════════════════════════════════════════════════════════════════════════
🎯 MAQSAD: PostgreSQL databazalarini to'liq boshqarish, monitoring va avtomatlashtirish
⚡ VERSIYA: 5.0.0 (ULTIMATE)
🏢 MUALLIF: DevOps Team
📋 LITSENZIYA: Enterprise
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
import hashlib# pyright: ignore[reportUnusedImport]
import logging
import datetime
import threading
import subprocess
import urllib.parse
import shutil
import pickle # pyright: ignore[reportUnusedImport]
import queue# pyright: ignore[reportUnusedImport]
import signal
import atexit
import platform # pyright: ignore[reportUnusedImport]
import socket # pyright: ignore[reportUnusedImport]
import ssl # pyright: ignore[reportUnusedImport]
import secrets
import string
import re
import csv
import ipaddress # pyright: ignore[reportUnusedImport]
import requests # pyright: ignore[reportUnusedImport]
import concurrent.futures
from typing import Dict, List, Tuple, Optional, Any, Union, Callable # pyright: ignore[reportUnusedImport]
from contextlib import contextmanager, closing
from dataclasses import dataclass, field, asdict # pyright: ignore[reportUnusedImport]
from enum import Enum, auto # pyright: ignore[reportUnusedImport]
from prettytable import PrettyTable # pyright: ignore[reportUnusedImport]
from colorama import init, Fore, Back, Style
import getpass # pyright: ignore[reportUnusedImport]
from functools import wraps
from abc import ABC, abstractmethod # pyright: ignore[reportUnusedImport]

# ============================================================================
# KONFIGURATSIYA - MAKSIMAL SAMARADORLIK UCHUN
# ============================================================================

init(autoreset=True)

@dataclass
class Config:
    """Global konfiguratsiya - 99% samaradorlik uchun optimallashtirilgan"""
    # Asosiy sozlamalar
    APP_NAME: str = "PostgreSQL Ultimate Enterprise"
    VERSION: str = "5.0.0"
    
    # Log va config papkalari
    BASE_DIR: str = "/opt/postgresql-enterprise"
    LOG_DIR: str = "/var/log/postgresql-ultimate"
    CONFIG_DIR: str = "/etc/postgresql-ultimate"
    BACKUP_DIR: str = "/var/backups/postgresql-ultimate"
    DATA_DIR: str = "/var/lib/postgresql-ultimate"
    TMP_DIR: str = "/tmp/postgresql-ultimate"
    
    # Fayllar
    LOG_FILE: str = ""
    DATABASE_URLS_FILE: str = ""
    DEPLOYMENTS_FILE: str = ""
    USERS_FILE: str = ""
    ROLES_FILE: str = ""
    METRICS_FILE: str = ""
    CACHE_FILE: str = ""
    SETTINGS_FILE: str = ""
    
    # Monitoring sozlamalari
    MONITOR_INTERVAL: int = 2  # sekund
    METRICS_RETENTION_DAYS: int = 30
    SLOW_QUERY_THRESHOLD: float = 0.5  # sekund
    ALERT_THRESHOLD_CONNECTIONS: int = 80
    ALERT_THRESHOLD_CPU: int = 70
    ALERT_THRESHOLD_MEMORY: int = 80
    ALERT_THRESHOLD_DISK: int = 85
    
    # Connection sozlamalari
    MAX_CONNECTIONS: int = 200
    CONNECTION_TIMEOUT: int = 10
    POOL_MIN_SIZE: int = 5
    POOL_MAX_SIZE: int = 50
    STATEMENT_TIMEOUT: int = 30000
    IDLE_TIMEOUT: int = 300
    
    # Security
    PASSWORD_MIN_LENGTH: int = 16
    PASSWORD_REQUIRE_UPPERCASE: bool = True
    PASSWORD_REQUIRE_LOWERCASE: bool = True
    PASSWORD_REQUIRE_DIGITS: bool = True
    PASSWORD_REQUIRE_SPECIAL: bool = True
    SESSION_TIMEOUT: int = 3600
    MAX_LOGIN_ATTEMPTS: int = 3
    
    # Performance
    CACHE_ENABLED: bool = True
    CACHE_TTL: int = 300
    PARALLEL_WORKERS: int = 4
    BATCH_SIZE: int = 1000
    COMPRESSION_LEVEL: int = 6
    
    # Features
    AUTO_BACKUP_ENABLED: bool = True
    AUTO_BACKUP_INTERVAL: int = 3600
    AUTO_CLEANUP_ENABLED: bool = True
    AUTO_CLEANUP_DAYS: int = 7
    TELEMETRY_ENABLED: bool = False
    
    def __post_init__(self):
        """Dinamik sozlamalarni initializatsiya qilish"""
        timestamp = datetime.datetime.now().strftime('%Y%m%d')
        self.LOG_FILE = f"{self.LOG_DIR}/pg_ultimate_{timestamp}.log" # pyright: ignore[reportConstantRedefinition, reportUnusedImport]
        self.DATABASE_URLS_FILE = f"{self.CONFIG_DIR}/database_urls.json" 
        self.DEPLOYMENTS_FILE = f"{self.CONFIG_DIR}/deployments.json" 
        self.USERS_FILE = f"{self.CONFIG_DIR}/users.json" # pyright: ignore[reportUnusedImport]
        self.ROLES_FILE = f"{self.CONFIG_DIR}/roles.json"
        self.METRICS_FILE = f"{self.CONFIG_DIR}/metrics.json"
        self.CACHE_FILE = f"{self.CONFIG_DIR}/cache.pickle"
        self.SETTINGS_FILE = f"{self.CONFIG_DIR}/settings.json"
        
        # Papkalarni yaratish
        for dir_path in [self.LOG_DIR, self.CONFIG_DIR, self.BACKUP_DIR, 
                         self.DATA_DIR, self.TMP_DIR]:
            os.makedirs(dir_path, mode=0o750, exist_ok=True)

config = Config()

# ============================================================================
# ENUMLAR VA TURLAR
# ============================================================================

class DeploymentType(Enum):
    """Deployment turlari"""
    LOCAL = "🏠 Local"
    REMOTE = "🌐 Remote"
    DOCKER = "🐳 Docker"
    KUBERNETES = "☸️ Kubernetes"
    CLOUD_AWS = "☁️ AWS RDS"
    CLOUD_GCP = "☁️ Google Cloud SQL"
    CLOUD_AZURE = "☁️ Azure DB"
    ON_PREMISE = "🏢 On-Premise"
    HYBRID = "🔄 Hybrid"

class ConnectionMode(Enum):
    """Ulanish rejimlari"""
    DIRECT = "⚡ Direct"
    POOL = "🔄 Connection Pool"
    SSL = "🔒 SSL/TLS"
    SSH_TUNNEL = "🔐 SSH Tunnel"
    PROXY = "🛡️ Proxy"
    LOAD_BALANCED = "⚖️ Load Balanced"

class EnvironmentType(Enum):
    """Muhit turlari"""
    DEVELOPMENT = "🛠️ Development"
    TESTING = "🧪 Testing"
    STAGING = "🎭 Staging"
    PRODUCTION = "🏭 Production"
    DISASTER_RECOVERY = "🚨 DR"
    SANDBOX = "📦 Sandbox"

class UserRole(Enum):
    """User rollari"""
    SUPERUSER = "👑 Superuser"
    ADMIN = "👨‍💼 Admin"
    DEVELOPER = "👨‍💻 Developer"
    READ_ONLY = "👀 Read Only"
    READ_WRITE = "✍️ Read/Write"
    ANALYST = "📊 Analyst"
    AUDITOR = "🔍 Auditor"
    CUSTOM = "⚙️ Custom"

class PermissionLevel(Enum):
    """Ruxsat darajalari"""
    NONE = "❌ None"
    CONNECT = "🔌 Connect"
    SELECT = "📖 Select"
    INSERT = "📝 Insert"
    UPDATE = "✏️ Update"
    DELETE = "🗑️ Delete"
    CREATE = "➕ Create"
    DROP = "➖ Drop"
    ALTER = "🔄 Alter"
    ALL = "✅ All"

class AlertLevel(Enum):
    """Alert darajalari"""
    INFO = "ℹ️ Info"
    WARNING = "⚠️ Warning"
    CRITICAL = "🔥 Critical"
    EMERGENCY = "🚨 Emergency"

class MetricType(Enum):
    """Metrik turlari"""
    CONNECTION = "🔌 Connection"
    PERFORMANCE = "⚡ Performance"
    RESOURCE = "💾 Resource"
    SECURITY = "🔒 Security"
    BACKUP = "💿 Backup"
    REPLICATION = "🔄 Replication"

# ============================================================================
# MAKSIMAL SAMARADORLIK UCHUN DEKORATORLAR
# ============================================================================

def perf_monitor(func):
    """Performance monitoring dekoratori"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        duration = (end - start) * 1000  # ms
        
        if duration > 100:  # 100ms dan katta
            logger.warning(f"⚡ Slow operation: {func.__name__} - {duration:.2f}ms")
        
        return result
    return wrapper

def cache_result(ttl: int = 300):
    """Natijalarni kesh qilish dekoratori"""
    def decorator(func):
        cache = {}
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(kwargs)
            now = time.time()
            if key in cache and now - cache[key]['time'] < ttl:
                return cache[key]['result']
            result = func(*args, **kwargs)
            cache[key] = {'result': result, 'time': now}
            return result
        return wrapper
    return decorator

def retry_on_failure(max_attempts: int = 3, delay: float = 0.5):
    """Xatolikda qayta urinish"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    time.sleep(delay * (attempt + 1))
            return None
        return wrapper
    return decorator

def async_executor(max_workers: int = 4):
    """Asinxron bajarish"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future = executor.submit(func, *args, **kwargs)
                return future
        return wrapper
    return decorator

# ============================================================================
# LOGGER - PROFESSIONAL LOGGING
# ============================================================================

class UltimateLogger:
    """Professional logging tizimi - 99.99% reliability"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.setup_logging()
            self.setup_rotation()
    
    def setup_logging(self):
        """Logging sozlamalari"""
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler
        fh = logging.FileHandler(config.LOG_FILE, encoding='utf-8')
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(logging.INFO)
        
        # Logger
        self.logger = logging.getLogger('PostgreSQL_Ultimate')
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
    
    def setup_rotation(self):
        """Log rotation"""
        if os.path.exists(config.LOG_FILE):
            if os.path.getsize(config.LOG_FILE) > 100 * 1024 * 1024:  # 100MB
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                rotated = f"{config.LOG_FILE}.{timestamp}"
                shutil.move(config.LOG_FILE, rotated)
                self.setup_logging()
    
    def debug(self, message):
        self.logger.debug(f"{Fore.CYAN}🔍 {message}{Style.RESET_ALL}")
    
    def info(self, message):
        self.logger.info(f"{Fore.CYAN}ℹ️ {message}{Style.RESET_ALL}")
    
    def success(self, message):
        self.logger.info(f"{Fore.GREEN}✅ {message}{Style.RESET_ALL}")
    
    def warning(self, message):
        self.logger.warning(f"{Fore.YELLOW}⚠️ {message}{Style.RESET_ALL}")
    
    def error(self, message):
        self.logger.error(f"{Fore.RED}❌ {message}{Style.RESET_ALL}")
    
    def critical(self, message):
        self.logger.critical(f"{Fore.RED}{Back.WHITE}🔥 {message}{Style.RESET_ALL}")

logger = UltimateLogger()

# ============================================================================
# DATABASE URL MANAGEMENT - ENTERPRISE GRADE
# ============================================================================

@dataclass
class DatabaseURL:
    """
    Enterprise Database URL management
    Format: postgresql://user:pass@host:port/db?param1=value1&param2=value2
    """
    scheme: str = "postgresql"
    username: str = None
    password: str = None
    host: str = "localhost"
    port: int = 5432
    database: str = None
    params: Dict[str, str] = field(default_factory=dict)
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    tags: List[str] = field(default_factory=list)
    description: str = ""
    
    @classmethod
    def from_string(cls, url: str) -> 'DatabaseURL':
        """URL string dan DatabaseURL obyekti yaratish"""
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
        """Database URL ni string qilib qaytarish"""
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
        """Connection parametrlari - optimallashtirilgan"""
        params = {
            'host': self.host,
            'port': self.port,
            'user': self.username,
            'password': self.password,
            'dbname': self.database,
            'connect_timeout': config.CONNECTION_TIMEOUT,
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5,
            'options': f'-c statement_timeout={config.STATEMENT_TIMEOUT} -c client_encoding=UTF8'
        }
        
        # SSL
        sslmode = self.params.get('sslmode', 'prefer')
        if sslmode:
            params['sslmode'] = sslmode
        
        for cert in ['sslrootcert', 'sslcert', 'sslkey']:
            if cert in self.params:
                params[cert] = self.params[cert]
        
        return params
    
    def test_connection(self) -> Tuple[bool, str]:
        """Ulanishni test qilish"""
        try:
            with closing(psycopg2.connect(**self.get_connection_params())) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True, "✅ Connection successful"
        except Exception as e:
            return False, f"❌ Connection failed: {str(e)}"
    
    def get_info(self) -> Dict[str, Any]:
        """URL haqida to'liq ma'lumot"""
        return {
            'scheme': self.scheme,
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'username': self.username,
            'params': self.params,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'tags': self.tags,
            'description': self.description
        }

# ============================================================================
# DATABASE URL MANAGER - WITH CACHE
# ============================================================================

class DatabaseURLManager:
    """Database URL manager - keshlash va optimizatsiya bilan"""
    
    def __init__(self):
        self.urls: Dict[str, DatabaseURL] = {}
        self.cache: Dict[str, Any] = {}
        self.load_urls()
    
    @perf_monitor
    def load_urls(self):
        """URL larni yuklash"""
        if os.path.exists(config.DATABASE_URLS_FILE):
            try:
                with open(config.DATABASE_URLS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for name, url_data in data.items():
                        url = DatabaseURL.from_string(url_data['url'])
                        url.tags = url_data.get('tags', [])
                        url.description = url_data.get('description', '')
                        url.created_at = datetime.datetime.fromisoformat(url_data.get('created_at', datetime.datetime.now().isoformat()))
                        url.updated_at = datetime.datetime.fromisoformat(url_data.get('updated_at', datetime.datetime.now().isoformat()))
                        self.urls[name] = url
                logger.success(f"📂 Loaded {len(self.urls)} database URLs")
            except Exception as e:
                logger.error(f"Failed to load URLs: {e}")
    
    @perf_monitor
    def save_urls(self):
        """URL larni saqlash"""
        try:
            data = {}
            for name, url in self.urls.items():
                data[name] = {
                    'url': url.to_string(),
                    'tags': url.tags,
                    'description': url.description,
                    'created_at': url.created_at.isoformat(),
                    'updated_at': url.updated_at.isoformat()
                }
            
            with open(config.DATABASE_URLS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.success("💾 Database URLs saved")
        except Exception as e:
            logger.error(f"Failed to save URLs: {e}")
    
    def add_url(self, name: str, url: DatabaseURL) -> bool:
        """Yangi URL qo'shish"""
        if name in self.urls:
            logger.warning(f"URL '{name}' already exists")
            return False
        
        url.updated_at = datetime.datetime.now()
        self.urls[name] = url
        self.save_urls()
        logger.success(f"✅ Added URL: {name} - {url.to_string(hide_password=True)}")
        return True
    
    def get_url(self, name: str) -> Optional[DatabaseURL]:
        """URL ni nomi bo'yicha olish"""
        return self.urls.get(name)
    
    def remove_url(self, name: str) -> bool:
        """URL o'chirish"""
        if name not in self.urls:
            logger.error(f"URL '{name}' not found")
            return False
        
        del self.urls[name]
        self.save_urls()
        logger.success(f"🗑️ Removed URL: {name}")
        return True
    
    def update_url(self, name: str, **kwargs) -> bool:
        """URL ni yangilash"""
        if name not in self.urls:
            logger.error(f"URL '{name}' not found")
            return False
        
        url = self.urls[name]
        for key, value in kwargs.items():
            if hasattr(url, key):
                setattr(url, key, value)
        
        url.updated_at = datetime.datetime.now()
        self.save_urls()
        logger.success(f"✏️ Updated URL: {name}")
        return True
    
    def list_urls(self) -> List[Tuple[str, DatabaseURL]]:
        """URL lar ro'yxati"""
        return list(self.urls.items())
    
    def search_urls(self, query: str) -> List[Tuple[str, DatabaseURL]]:
        """URL larni qidirish"""
        query = query.lower()
        results = []
        
        for name, url in self.urls.items():
            if query in name.lower() or \
               (url.database and query in url.database.lower()) or \
               (url.host and query in url.host.lower()) or \
               any(query in tag.lower() for tag in url.tags):
                results.append((name, url))
        
        return results

# ============================================================================
# POSTGRESQL MANAGER - CORE FUNCTIONALITY
# ============================================================================

class PostgreSQLManager:
    """PostgreSQL core manager - optimallashtirilgan"""
    
    def __init__(self, database_url: DatabaseURL = None):
        self.database_url = database_url
        self.connection_pool = None
        self.monitoring_active = False
        self.monitor_thread = None
        self.metrics_history: List[Dict] = []
        self.alerts: List[Dict] = []
        self.cache: Dict[str, Any] = {}
        
        if database_url:
            self.create_pool()
    
    @perf_monitor
    def create_pool(self):
        """Connection pool yaratish"""
        try:
            params = self.database_url.get_connection_params()
            params['application_name'] = 'PostgreSQL_Ultimate'
            
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                config.POOL_MIN_SIZE,
                config.POOL_MAX_SIZE,
                **params
            )
            
            logger.success(f"🔄 Connection pool created: {config.POOL_MIN_SIZE}-{config.POOL_MAX_SIZE}")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    @contextmanager
    @retry_on_failure(max_attempts=3)
    def get_connection(self):
        """Connection olish"""
        conn = None
        try:
            if self.connection_pool:
                conn = self.connection_pool.getconn()
            else:
                conn = psycopg2.connect(**self.database_url.get_connection_params())
            
            yield conn
        finally:
            if conn:
                if self.connection_pool:
                    self.connection_pool.putconn(conn)
                else:
                    conn.close()
    
    @contextmanager
    @retry_on_failure(max_attempts=3)
    def get_cursor(self, cursor_factory=psycopg2.extras.RealDictCursor):
        """Cursor olish"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=cursor_factory) as cursor:
                yield cursor
                conn.commit()
    
    @perf_monitor
    def execute_query(self, query: str, params: tuple = None, 
                     fetch: bool = True) -> Optional[List[Dict]]:
        """Query bajarish - optimallashtirilgan"""
        start_time = time.time()
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                
                if fetch and cursor.description:
                    result = cursor.fetchall()
                    duration = time.time() - start_time
                    
                    if duration > config.SLOW_QUERY_THRESHOLD:
                        logger.warning(f"🐌 Slow query ({duration:.2f}s): {query[:100]}...")
                    
                    return result
                return None
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    @perf_monitor
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Ko'p querylarni bajarish"""
        count = 0
        try:
            with self.get_cursor() as cursor:
                cursor.executemany(query, params_list)
                count = cursor.rowcount
            return count
        except Exception as e:
            logger.error(f"Execute many failed: {e}")
            raise
    
    @perf_monitor
    def transaction(self, queries: List[tuple]) -> bool:
        """Transaction bajarish"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for query, params in queries:
                        cursor.execute(query, params)
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            return False
    
    # ========================================================================
    # DATABASE MANAGEMENT
    # ========================================================================
    
    @perf_monitor
    def create_database(self, db_name: str, owner: str = None, 
                       encoding: str = 'UTF8') -> bool:
        """Yangi database yaratish"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                if cursor.fetchone():
                    logger.warning(f"Database '{db_name}' already exists")
                    return False
                
                if owner:
                    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (owner,))
                    if not cursor.fetchone():
                        logger.error(f"User '{owner}' not found")
                        return False
                
                cursor.execute(f"CREATE DATABASE {db_name} ENCODING '{encoding}'")
                
                if owner:
                    cursor.execute(f"ALTER DATABASE {db_name} OWNER TO {owner}")
                    cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {owner}")
                
                logger.success(f"📁 Database created: {db_name}")
                return True
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            return False
    
    @perf_monitor
    def drop_database(self, db_name: str, force: bool = False) -> bool:
        """Database o'chirish"""
        if db_name in ['postgres', 'template0', 'template1']:
            logger.error("Cannot drop system database")
            return False
        
        try:
            with self.get_cursor() as cursor:
                if force:
                    cursor.execute("""
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = %s AND pid != pg_backend_pid()
                    """, (db_name,))
                
                cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                logger.success(f"🗑️ Database dropped: {db_name}")
                return True
        except Exception as e:
            logger.error(f"Failed to drop database: {e}")
            return False
    
    @perf_monitor
    @cache_result(ttl=60)
    def list_databases(self) -> List[Dict]:
        """Database lar ro'yxati"""
        query = """
            SELECT 
                d.datname,
                pg_get_userbyid(d.datdba) as owner,
                pg_size_pretty(pg_database_size(d.datname)) as size,
                pg_database_size(d.datname) as size_bytes,
                d.datconnlimit as connection_limit,
                (SELECT count(*) FROM pg_stat_activity WHERE datname = d.datname) as connections,
                d.datistemplate as is_template,
                d.datallowconn as allow_connections,
                pg_encoding_to_char(d.encoding) as encoding,
                d.datcollate as collate,
                d.datctype as ctype,
                d.datfrozenxid::text as frozen_xid,
                d.datminmxid::text as min_mxid,
                pg_size_pretty(sum(pg_relation_size(oid))::bigint) as total_table_size
            FROM pg_database d
            LEFT JOIN pg_class c ON c.reltablespace = d.oid
            WHERE d.datistemplate = false
            GROUP BY d.oid, d.datname, d.datdba, d.datconnlimit, d.datistemplate, 
                     d.datallowconn, d.encoding, d.datcollate, d.datctype, 
                     d.datfrozenxid, d.datminmxid
            ORDER BY size_bytes DESC
        """
        
        return self.execute_query(query) or []
    
    # ========================================================================
    # USER MANAGEMENT
    # ========================================================================
    
    @perf_monitor
    def create_user(self, username: str, password: str = None,
                   superuser: bool = False, createdb: bool = False,
                   createrole: bool = False, login: bool = True,
                   connection_limit: int = -1, valid_until: str = None,
                   role: UserRole = UserRole.READ_WRITE) -> bool:
        """Yangi user yaratish"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (username,))
                if cursor.fetchone():
                    logger.warning(f"User '{username}' already exists")
                    return False
                
                if not password:
                    password = self._generate_strong_password()
                    logger.info(f"Generated password: {password}")
                
                if not self._validate_password(password):
                    logger.error("Password does not meet security requirements")
                    return False
                
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
                
                cursor.execute(f"CREATE USER {username} WITH " + " ".join(options))
                
                # Default ruxsatlar
                if role == UserRole.SUPERUSER:
                    cursor.execute(f"ALTER USER {username} WITH SUPERUSER")
                elif role == UserRole.READ_ONLY:
                    cursor.execute(f"GRANT CONNECT ON DATABASE postgres TO {username}")
                    cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {username}")
                    cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s", (username,))
                elif role == UserRole.READ_WRITE:
                    cursor.execute(f"GRANT CONNECT, CREATE ON DATABASE postgres TO {username}")
                    cursor.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {username}")
                    cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s", (username,))
                
                logger.success(f"👤 User created: {username} (Role: {role.value})")
                return True
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            return False
    
    @perf_monitor
    def drop_user(self, username: str, reassign_to: str = None) -> bool:
        """User o'chirish"""
        if username == 'postgres':
            logger.error("Cannot drop postgres superuser")
            return False
        
        try:
            with self.get_cursor() as cursor:
                if reassign_to:
                    cursor.execute(f"REASSIGN OWNED BY {username} TO {reassign_to}")
                    cursor.execute(f"DROP OWNED BY {username}")
                
                cursor.execute(f"DROP USER IF EXISTS {username}")
                logger.success(f"🗑️ User dropped: {username}")
                return True
        except Exception as e:
            logger.error(f"Failed to drop user: {e}")
            return False
    
    @perf_monitor
    @cache_result(ttl=30)
    def list_users(self) -> List[Dict]:
        """Userlar ro'yxati"""
        query = """
            SELECT 
                rolname as username,
                rolsuper as is_superuser,
                rolcreatedb as can_create_db,
                rolcreaterole as can_create_role,
                rolinherit as inherit_rights,
                rolcanlogin as can_login,
                rolconnlimit as connection_limit,
                rolvaliduntil as valid_until,
                (SELECT count(*) FROM pg_stat_activity WHERE usename = rolname) as active_connections,
                array_agg(DISTINCT datname) as databases_accessed,
                array_agg(DISTINCT grantee) as granted_roles
            FROM pg_roles r
            LEFT JOIN pg_stat_activity a ON a.usename = r.rolname
            LEFT JOIN information_schema.table_privileges p ON p.grantee = r.rolname
            WHERE rolname NOT LIKE 'pg_%'
            GROUP BY r.oid, rolname, rolsuper, rolcreatedb, rolcreaterole, 
                     rolinherit, rolcanlogin, rolconnlimit, rolvaliduntil
            ORDER BY rolname
        """
        
        return self.execute_query(query) or []
    
    def _generate_strong_password(self, length: int = None) -> str:
        """Kuchli parol generatsiya qilish"""
        if length is None:
            length = config.PASSWORD_MIN_LENGTH
        
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*()_+-=[]{}|;:,.<>?"
        
        while True:
            password = ''.join(secrets.choice(alphabet) for _ in range(length))
            
            if self._validate_password(password):
                return password
    
    def _validate_password(self, password: str) -> bool:
        """Parol kuchliligini tekshirish"""
        if len(password) < config.PASSWORD_MIN_LENGTH:
            return False
        
        if config.PASSWORD_REQUIRE_UPPERCASE and not re.search(r"[A-Z]", password):
            return False
        
        if config.PASSWORD_REQUIRE_LOWERCASE and not re.search(r"[a-z]", password):
            return False
        
        if config.PASSWORD_REQUIRE_DIGITS and not re.search(r"\d", password):
            return False
        
        if config.PASSWORD_REQUIRE_SPECIAL and not re.search(r"[!@#$%^&*()_+\-=\[\]{}|;:,.<>?]", password):
            return False
        
        return True
    
    # ========================================================================
    # PRIVILEGES MANAGEMENT
    # ========================================================================
    
    @perf_monitor
    def grant_privileges(self, username: str, db_name: str = None,
                        schema: str = 'public', 
                        privileges: List[str] = None,
                        object_type: str = 'ALL') -> bool:
        """Ruxsatlar berish"""
        try:
            with self.get_cursor(db_name=db_name) as cursor:
                priv_str = ','.join(privileges) if privileges else 'ALL'
                
                if object_type.upper() == 'ALL':
                    object_types = ['TABLES', 'SEQUENCES', 'FUNCTIONS', 'TYPES', 'SCHEMAS']
                else:
                    object_types = [object_type]
                
                for obj_type in object_types:
                    # Default privileges
                    cursor.execute(f"""
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema}
                        GRANT {priv_str} ON {obj_type} TO {username}
                    """)
                    
                    # Existing objects
                    cursor.execute(f"""
                        GRANT {priv_str} ON ALL {obj_type} IN SCHEMA {schema} TO {username}
                    """)
                
                if db_name:
                    cursor.execute(f"GRANT CONNECT ON DATABASE {db_name} TO {username}")
                    cursor.execute(f"GRANT TEMPORARY ON DATABASE {db_name} TO {username}")
                
                logger.success(f"🔐 Granted privileges to: {username}")
                return True
        except Exception as e:
            logger.error(f"Failed to grant privileges: {e}")
            return False
    
    @perf_monitor
    def revoke_privileges(self, username: str, db_name: str = None,
                         schema: str = 'public',
                         privileges: List[str] = None) -> bool:
        """Ruxsatlarni olib tashlash"""
        try:
            with self.get_cursor(db_name=db_name) as cursor:
                priv_str = ','.join(privileges) if privileges else 'ALL'
                
                cursor.execute(f"""
                    REVOKE {priv_str} ON ALL TABLES IN SCHEMA {schema} FROM {username}
                """)
                cursor.execute(f"""
                    REVOKE {priv_str} ON ALL SEQUENCES IN SCHEMA {schema} FROM {username}
                """)
                cursor.execute(f"""
                    REVOKE {priv_str} ON ALL FUNCTIONS IN SCHEMA {schema} FROM {username}
                """)
                
                if db_name:
                    cursor.execute(f"REVOKE CONNECT ON DATABASE {db_name} FROM {username}")
                
                logger.success(f"🔒 Revoked privileges from: {username}")
                return True
        except Exception as e:
            logger.error(f"Failed to revoke privileges: {e}")
            return False
    
    @perf_monitor
    @cache_result(ttl=60)
    def get_user_privileges(self, username: str) -> List[Dict]:
        """User ruxsatlarini olish"""
        privileges = []
        
        with self.get_cursor() as cursor:
            # Database privileges
            cursor.execute("""
                SELECT 
                    datname,
                    has_database_privilege(%s, datname, 'CONNECT') as can_connect,
                    has_database_privilege(%s, datname, 'CREATE') as can_create,
                    has_database_privilege(%s, datname, 'TEMPORARY') as can_temp
                FROM pg_database
                WHERE datistemplate = false
            """, (username, username, username))
            
            for row in cursor.fetchall():
                privileges.append({
                    'type': 'database',
                    'name': row['datname'],
                    'privileges': {
                        'connect': row['can_connect'],
                        'create': row['can_create'],
                        'temporary': row['can_temp']
                    }
                })
            
            # Table privileges
            cursor.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    has_table_privilege(%s, schemaname||'.'||tablename, 'SELECT') as can_select,
                    has_table_privilege(%s, schemaname||'.'||tablename, 'INSERT') as can_insert,
                    has_table_privilege(%s, schemaname||'.'||tablename, 'UPDATE') as can_update,
                    has_table_privilege(%s, schemaname||'.'||tablename, 'DELETE') as can_delete,
                    has_table_privilege(%s, schemaname||'.'||tablename, 'TRUNCATE') as can_truncate,
                    has_table_privilege(%s, schemaname||'.'||tablename, 'REFERENCES') as can_reference,
                    has_table_privilege(%s, schemaname||'.'||tablename, 'TRIGGER') as can_trigger
                FROM pg_tables
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                LIMIT 100
            """, (username, username, username, username, username, username, username))
            
            for row in cursor.fetchall():
                privileges.append({
                    'type': 'table',
                    'schema': row['schemaname'],
                    'name': row['tablename'],
                    'privileges': {
                        'select': row['can_select'],
                        'insert': row['can_insert'],
                        'update': row['can_update'],
                        'delete': row['can_delete'],
                        'truncate': row['can_truncate'],
                        'references': row['can_reference'],
                        'trigger': row['can_trigger']
                    }
                })
        
        return privileges
    
    # ========================================================================
    # ROLES MANAGEMENT
    # ========================================================================
    
    @perf_monitor
    def create_role(self, role_name: str, parent_role: str = None,
                   privileges: List[str] = None) -> bool:
        """Yangi rol yaratish"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(f"CREATE ROLE {role_name}")
                
                if parent_role:
                    cursor.execute(f"GRANT {parent_role} TO {role_name}")
                
                if privileges:
                    for priv in privileges:
                        cursor.execute(f"GRANT {priv} TO {role_name}")
                
                logger.success(f"👥 Role created: {role_name}")
                return True
        except Exception as e:
            logger.error(f"Failed to create role: {e}")
            return False
    
    @perf_monitor
    def assign_role(self, username: str, role_name: str) -> bool:
        """Rol biriktirish"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(f"GRANT {role_name} TO {username}")
                logger.success(f"🔗 Role '{role_name}' assigned to '{username}'")
                return True
        except Exception as e:
            logger.error(f"Failed to assign role: {e}")
            return False
    
    @perf_monitor
    def revoke_role(self, username: str, role_name: str) -> bool:
        """Rolni olib tashlash"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(f"REVOKE {role_name} FROM {username}")
                logger.success(f"🔓 Role '{role_name}' revoked from '{username}'")
                return True
        except Exception as e:
            logger.error(f"Failed to revoke role: {e}")
            return False
    
    @perf_monitor
    @cache_result(ttl=60)
    def list_roles(self) -> List[Dict]:
        """Rollar ro'yxati"""
        query = """
            SELECT 
                rolname as role_name,
                rolsuper as is_superuser,
                rolcreatedb as can_create_db,
                rolcreaterole as can_create_role,
                rolinherit as inherit_rights,
                rolcanlogin as can_login,
                (SELECT count(*) FROM pg_auth_members WHERE roleid = r.oid) as member_count,
                array_agg(m.rolname) as members
            FROM pg_roles r
            LEFT JOIN pg_auth_members am ON am.roleid = r.oid
            LEFT JOIN pg_roles m ON m.oid = am.member
            WHERE r.rolname NOT LIKE 'pg_%'
            GROUP BY r.oid, rolname, rolsuper, rolcreatedb, rolcreaterole, rolinherit, rolcanlogin
            ORDER BY rolname
        """
        
        return self.execute_query(query) or []
    
    # ========================================================================
    # MONITORING AND METRICS
    # ========================================================================
    
    @perf_monitor
    def get_metrics(self) -> Dict[str, Any]:
        """PostgreSQL metrikalari"""
        metrics = {}
        
        with self.get_cursor() as cursor:
            # Connections
            cursor.execute("""
                SELECT 
                    count(*) as total_connections,
                    count(*) FILTER (WHERE state = 'active') as active_connections,
                    count(*) FILTER (WHERE state = 'idle') as idle_connections,
                    count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
                    count(*) FILTER (WHERE wait_event IS NOT NULL) as waiting_connections,
                    count(DISTINCT datname) as active_databases,
                    count(DISTINCT usename) as active_users,
                    max(age(now(), query_start)) as longest_query
                FROM pg_stat_activity
            """)
            metrics['connections'] = cursor.fetchone()
            
            # Database sizes
            cursor.execute("""
                SELECT 
                    count(*) as database_count,
                    pg_size_pretty(sum(pg_database_size(datname))) as total_size,
                    sum(pg_database_size(datname)) as total_size_bytes
                FROM pg_database
                WHERE datistemplate = false
            """)
            metrics['databases'] = cursor.fetchone()
            
            # Cache hit ratio
            cursor.execute("""
                SELECT 
                    sum(heap_blks_hit)::float / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100 as cache_hit_ratio,
                    sum(idx_blks_hit)::float / nullif(sum(idx_blks_hit) + sum(idx_blks_read), 0) * 100 as index_cache_ratio,
                    sum(toast_blks_hit)::float / nullif(sum(toast_blks_hit) + sum(toast_blks_read), 0) * 100 as toast_cache_ratio
                FROM pg_statio_user_tables
            """)
            metrics['cache'] = cursor.fetchone()
            
            # Index usage
            cursor.execute("""
                SELECT 
                    sum(idx_scan) as total_index_scans,
                    sum(idx_tup_fetch) as total_index_fetches,
                    sum(idx_tup_read) as total_index_reads,
                    sum(idx_scan)::float / nullif(sum(idx_scan) + sum(seq_scan), 0) * 100 as index_usage_ratio
                FROM pg_stat_user_tables
            """)
            metrics['indexes'] = cursor.fetchone()
            
            # Locks
            cursor.execute("""
                SELECT 
                    locktype,
                    mode,
                    count(*) as lock_count,
                    count(DISTINCT relation) as locked_tables,
                    count(DISTINCT pid) as waiting_pids
                FROM pg_locks
                WHERE granted = false
                GROUP BY locktype, mode
            """)
            metrics['locks'] = cursor.fetchall()
            
            # Transactions
            cursor.execute("""
                SELECT 
                    xact_commit,
                    xact_rollback,
                    xact_commit + xact_rollback as total_transactions,
                    xact_commit::float / nullif(xact_commit + xact_rollback, 0) * 100 as commit_ratio
                FROM pg_stat_database
                WHERE datname = current_database()
            """)
            metrics['transactions'] = cursor.fetchone()
            
            # Replication
            cursor.execute("""
                SELECT 
                    count(*) as replication_slots,
                    count(*) FILTER (WHERE active) as active_slots,
                    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as replication_lag_bytes,
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)) as replication_lag
                FROM pg_stat_replication
            """)
            metrics['replication'] = cursor.fetchone()
            
            # Background writer
            cursor.execute("""
                SELECT 
                    buffers_checkpoint,
                    buffers_clean,
                    maxwritten_clean,
                    buffers_backend,
                    buffers_alloc,
                    buffers_backend_fsync
                FROM pg_stat_bgwriter
            """)
            metrics['bgwriter'] = cursor.fetchone()
        
        metrics['timestamp'] = datetime.datetime.now().isoformat()
        return metrics
    
    @perf_monitor
    def get_slow_queries(self, threshold: float = None) -> List[Dict]:
        """Sekin querylarni olish"""
        if threshold is None:
            threshold = config.SLOW_QUERY_THRESHOLD
        
        query = """
            SELECT 
                pid,
                usename as username,
                datname as database,
                query,
                state,
                age(now(), query_start) as duration,
                wait_event_type || ': ' || wait_event as waiting,
                query_start,
                xact_start,
                application_name,
                client_addr,
                client_hostname,
                client_port,
                backend_type
            FROM pg_stat_activity
            WHERE state = 'active' 
                AND query NOT LIKE '%pg_stat_activity%'
                AND age(now(), query_start) > interval %s
                AND pid != pg_backend_pid()
            ORDER BY duration DESC
        """
        
        threshold_interval = f"{threshold} seconds"
        return self.execute_query(query, (threshold_interval,)) or []
    
    @perf_monitor
    def get_table_sizes(self, limit: int = 20) -> List[Dict]:
        """Table o'lchamlari"""
        query = """
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
                pg_total_relation_size(schemaname||'.'||tablename) as total_bytes,
                pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                              pg_relation_size(schemaname||'.'||tablename)) as index_size,
                n_live_tup as live_rows,
                n_dead_tup as dead_rows,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze,
                vacuum_count,
                autovacuum_count,
                analyze_count,
                autoanalyze_count
            FROM pg_tables t
            JOIN pg_stat_user_tables s ON t.tablename = s.relname
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            LIMIT %s
        """
        
        return self.execute_query(query, (limit,)) or []
    
    # ========================================================================
    # BACKUP AND RESTORE
    # ========================================================================
    
    @perf_monitor
    def backup_database(self, db_name: str, backup_type: str = 'full',
                       compress: bool = True) -> Optional[str]:
        """Database backup"""
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"{config.BACKUP_DIR}/{db_name}_{timestamp}"
        
        try:
            env = os.environ.copy()
            if self.database_url.password:
                env['PGPASSWORD'] = self.database_url.password
            
            if backup_type == 'full':
                backup_file += '.dump'
                cmd = [
                    'pg_dump',
                    '-h', self.database_url.host,
                    '-p', str(self.database_url.port),
                    '-U', self.database_url.username,
                    '-F', 'c',
                    '-f', backup_file,
                    db_name
                ]
            elif backup_type == 'schema':
                backup_file += '_schema.sql'
                cmd = [
                    'pg_dump',
                    '-h', self.database_url.host,
                    '-p', str(self.database_url.port),
                    '-U', self.database_url.username,
                    '-s',
                    '-f', backup_file,
                    db_name
                ]
            elif backup_type == 'data':
                backup_file += '_data.sql'
                cmd = [
                    'pg_dump',
                    '-h', self.database_url.host,
                    '-p', str(self.database_url.port),
                    '-U', self.database_url.username,
                    '-a',
                    '-f', backup_file,
                    db_name
                ]
            else:
                backup_file += '.sql'
                cmd = [
                    'pg_dump',
                    '-h', self.database_url.host,
                    '-p', str(self.database_url.port),
                    '-U', self.database_url.username,
                    '-f', backup_file,
                    db_name
                ]
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                if compress:
                    import gzip
                    with open(backup_file, 'rb') as f_in:
                        with gzip.open(f"{backup_file}.gz", 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    os.remove(backup_file)
                    backup_file += '.gz'
                
                size = os.path.getsize(backup_file)
                logger.success(f"💾 Backup created: {os.path.basename(backup_file)} ({size/1024/1024:.2f} MB)")
                return backup_file
            else:
                logger.error(f"Backup failed: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return None
    
    @perf_monitor
    def restore_database(self, db_name: str, backup_file: str) -> bool:
        """Database restore"""
        if not os.path.exists(backup_file):
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        try:
            # Decompress if gzipped
            if backup_file.endswith('.gz'):
                import gzip
                decompressed = backup_file[:-3]
                with gzip.open(backup_file, 'rb') as f_in:
                    with open(decompressed, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                backup_file = decompressed
            
            # Terminate connections
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = %s AND pid != pg_backend_pid()
                """, (db_name,))
                
                cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                cursor.execute(f"CREATE DATABASE {db_name}")
            
            # Restore
            env = os.environ.copy()
            if self.database_url.password:
                env['PGPASSWORD'] = self.database_url.password
            
            if backup_file.endswith('.dump'):
                cmd = [
                    'pg_restore',
                    '-h', self.database_url.host,
                    '-p', str(self.database_url.port),
                    '-U', self.database_url.username,
                    '-d', db_name,
                    '-v',
                    backup_file
                ]
            else:
                cmd = [
                    'psql',
                    '-h', self.database_url.host,
                    '-p', str(self.database_url.port),
                    '-U', self.database_url.username,
                    '-d', db_name,
                    '-f', backup_file
                ]
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.success(f"🔄 Database restored: {db_name}")
                return True
            else:
                logger.error(f"Restore failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
    
    # ========================================================================
    # DATA INSERTION
    # ========================================================================
    
    @perf_monitor
    def insert_data(self, table: str, data: Union[Dict, List[Dict]],
                   batch_size: int = None) -> Tuple[int, int]:
        """Ma'lumot qo'shish"""
        if isinstance(data, dict):
            data = [data]
        
        if not data:
            return 0, 0
        
        if batch_size is None:
            batch_size = config.BATCH_SIZE
        
        successful = 0
        failed = 0
        
        columns = list(data[0].keys())
        placeholders = ','.join(['%s'] * len(columns))
        query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        
        try:
            with self.get_cursor() as cursor:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    values = [tuple(row[col] for col in columns) for row in batch]
                    
                    try:
                        cursor.executemany(query, values)
                        successful += len(batch)
                    except Exception as e:
                        logger.error(f"Batch insert failed: {e}")
                        failed += len(batch)
            
            logger.success(f"📝 Inserted {successful} rows into {table}")
            return successful, failed
        except Exception as e:
            logger.error(f"Insert failed: {e}")
            return 0, len(data)
    
    @perf_monitor
    def import_csv(self, table: str, csv_file: str, delimiter: str = ',',
                  header: bool = True) -> Tuple[int, int]:
        """CSV fayldan import"""
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                if header:
                    reader = csv.DictReader(f, delimiter=delimiter)
                    data = list(reader)
                else:
                    reader = csv.reader(f, delimiter=delimiter)
                    headers = [f"column_{i}" for i in range(len(next(reader)))]
                    f.seek(0)
                    reader = csv.DictReader(f, fieldnames=headers, delimiter=delimiter)
                    data = list(reader)
                
                # Convert types
                for row in data:
                    for key, value in row.items():
                        if value is not None and value != '':
                            if value.lower() in ('true', 'false'):
                                row[key] = value.lower() == 'true'
                            elif value.replace('.', '').replace('-', '').isdigit():
                                if '.' in value:
                                    row[key] = float(value)
                                else:
                                    row[key] = int(value)
                
                return self.insert_data(table, data)
        except Exception as e:
            logger.error(f"CSV import failed: {e}")
            return 0, 0
    
    @perf_monitor
    def import_json(self, table: str, json_file: str) -> Tuple[int, int]:
        """JSON fayldan import"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                if isinstance(data, dict):
                    data = [data]
                
                return self.insert_data(table, data)
        except Exception as e:
            logger.error(f"JSON import failed: {e}")
            return 0, 0
    
    # ========================================================================
    # MONITORING THREAD
    # ========================================================================
    
    def start_monitoring(self):
        """Monitoring thread ni boshlash"""
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.success("📊 Monitoring started")
    
    def stop_monitoring(self):
        """Monitoring thread ni to'xtatish"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("📊 Monitoring stopped")
    
    def _monitoring_loop(self):
        """Monitoring asosiy loop"""
        while self.monitoring_active:
            try:
                metrics = self.get_metrics()
                self.metrics_history.append({
                    'timestamp': datetime.datetime.now(),
                    'metrics': metrics
                })
                
                # Clean old metrics
                cutoff = datetime.datetime.now() - datetime.timedelta(days=config.METRICS_RETENTION_DAYS)
                self.metrics_history = [
                    m for m in self.metrics_history 
                    if m['timestamp'] > cutoff
                ]
                
                # Check alerts
                self._check_alerts(metrics)
                
                time.sleep(config.MONITOR_INTERVAL)
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
    
    def _check_alerts(self, metrics: Dict[str, Any]):
        """Alert larni tekshirish"""
        now = datetime.datetime.now()
        
        # Connection alerts
        if metrics['connections']['total_connections'] > config.ALERT_THRESHOLD_CONNECTIONS:
            self.alerts.append({
                'timestamp': now,
                'level': AlertLevel.WARNING,
                'type': 'connections',
                'message': f"High connections: {metrics['connections']['total_connections']}",
                'value': metrics['connections']['total_connections'],
                'threshold': config.ALERT_THRESHOLD_CONNECTIONS
            })
        
        # Cache hit ratio alerts
        if metrics['cache'] and metrics['cache']['cache_hit_ratio'] < 95:
            self.alerts.append({
                'timestamp': now,
                'level': AlertLevel.WARNING,
                'type': 'cache',
                'message': f"Low cache hit ratio: {metrics['cache']['cache_hit_ratio']:.1f}%",
                'value': metrics['cache']['cache_hit_ratio'],
                'threshold': 95
            })
        
        # Slow queries alerts
        slow_queries = self.get_slow_queries(threshold=5.0)
        if slow_queries:
            self.alerts.append({
                'timestamp': now,
                'level': AlertLevel.WARNING,
                'type': 'slow_queries',
                'message': f"Slow queries detected: {len(slow_queries)}",
                'value': len(slow_queries),
                'threshold': 0
            })
        
        # Keep only last 100 alerts
        if len(self.alerts) > 100:
            self.alerts = self.alerts[-100:]
    
    def close(self):
        """Resurslarni tozalash"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("🔌 Connection pool closed")

# ============================================================================
# DEPLOYMENT MANAGER
# ============================================================================

class DeploymentManager:
    """Deployment management system"""
    
    def __init__(self):
        self.deployments: Dict[str, Dict[str, Any]] = {}
        self.load_deployments()
    
    @perf_monitor
    def load_deployments(self):
        """Deployment larni yuklash"""
        if os.path.exists(config.DEPLOYMENTS_FILE):
            try:
                with open(config.DEPLOYMENTS_FILE, 'r', encoding='utf-8') as f:
                    self.deployments = json.load(f)
                logger.success(f"📦 Loaded {len(self.deployments)} deployments")
            except Exception as e:
                logger.error(f"Failed to load deployments: {e}")
    
    @perf_monitor
    def save_deployments(self):
        """Deployment larni saqlash"""
        try:
            with open(config.DEPLOYMENTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.deployments, f, indent=2, ensure_ascii=False)
            logger.success("💾 Deployments saved")
        except Exception as e:
            logger.error(f"Failed to save deployments: {e}")
    
    def create_deployment(self, name: str, url_name: str,
                         environment: EnvironmentType,
                         deployment_type: DeploymentType,
                         connection_mode: ConnectionMode,
                         **kwargs) -> bool:
        """Yangi deployment yaratish"""
        if name in self.deployments:
            logger.warning(f"Deployment '{name}' already exists")
            return False
        
        self.deployments[name] = {
            'name': name,
            'url_name': url_name,
            'environment': environment.value,
            'deployment_type': deployment_type.value,
            'connection_mode': connection_mode.value,
            'created_at': datetime.datetime.now().isoformat(),
            'updated_at': datetime.datetime.now().isoformat(),
            'status': 'created',
            'config': kwargs,
            'metrics': []
        }
        
        self.save_deployments()
        logger.success(f"🚀 Deployment created: {name}")
        return True
    
    def get_deployment(self, name: str) -> Optional[Dict]:
        """Deployment olish"""
        return self.deployments.get(name)
    
    def remove_deployment(self, name: str) -> bool:
        """Deployment o'chirish"""
        if name not in self.deployments:
            logger.error(f"Deployment '{name}' not found")
            return False
        
        del self.deployments[name]
        self.save_deployments()
        logger.success(f"🗑️ Deployment removed: {name}")
        return True
    
    def update_deployment_status(self, name: str, status: str):
        """Deployment status yangilash"""
        if name in self.deployments:
            self.deployments[name]['status'] = status
            self.deployments[name]['updated_at'] = datetime.datetime.now().isoformat()
            self.save_deployments()
    
    def add_metrics(self, name: str, metrics: Dict):
        """Metrika qo'shish"""
        if name in self.deployments:
            self.deployments[name]['metrics'].append({
                'timestamp': datetime.datetime.now().isoformat(),
                'metrics': metrics
            })
            
            # Keep only last 1000 metrics
            if len(self.deployments[name]['metrics']) > 1000:
                self.deployments[name]['metrics'] = self.deployments[name]['metrics'][-1000:]
            
            self.save_deployments()
    
    def list_deployments(self) -> List[Tuple[str, Dict]]:
        """Deployment lar ro'yxati"""
        return list(self.deployments.items())
    
    def list_by_environment(self, environment: EnvironmentType) -> List[Dict]:
        """Muhit bo'yicha deployment lar"""
        env_value = environment.value
        return [d for d in self.deployments.values() if d['environment'] == env_value]

# ============================================================================
# ULTIMATE UI - IKKI QISIMGA BO'LINGAN MENYU
# ============================================================================

class UltimateUI:
    """Asosiy interfeys - ikkita menyu qismi"""
    
    def __init__(self):
        self.url_manager = DatabaseURLManager()
        self.deployment_manager = DeploymentManager()
        self.current_pg_manager: Optional[PostgreSQLManager] = None
        self.current_db_url: Optional[DatabaseURL] = None
        self.current_deployment: Optional[str] = None
        self.running = True
        self.menu_section = 1  # 1 - Database Management, 2 - Deployment & Monitoring
        self.performance_mode = True
        self.theme = 'dark'
        
        # Load saved state
        self.load_state()
    
    def load_state(self):
        """Saqlangan holatni yuklash"""
        state_file = f"{config.CONFIG_DIR}/ui_state.json"
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    self.menu_section = state.get('menu_section', 1)
                    self.performance_mode = state.get('performance_mode', True)
                    self.theme = state.get('theme', 'dark')
            except:
                pass
    
    def save_state(self):
        """Holatni saqlash"""
        state_file = f"{config.CONFIG_DIR}/ui_state.json"
        try:
            with open(state_file, 'w') as f:
                json.dump({
                    'menu_section': self.menu_section,
                    'performance_mode': self.performance_mode,
                    'theme': self.theme
                }, f)
        except:
            pass
    
    def clear_screen(self):
        """Ekran tozalash"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def print_ultimate_header(self):
        """Ultimate header - 99% samaradorlik"""
        self.clear_screen()
        
        # ASCII Art Logo
        logo = f"""
{Fore.CYAN}╔════════════════════════════════════════════════════════════════════════════════════════════╗
{Fore.GREEN}║                                                                                            ║
{Fore.GREEN}║     ██████╗  ██████╗ ███████╗████████╗ ██████╗ ██████╗ ███████╗ ██████╗ ██╗            ║
{Fore.GREEN}║     ██╔══██╗██╔═══██╗██╔════╝╚══██╔══╝██╔════╝ ██╔══██╗██╔════╝██╔═══██╗██║            ║
{Fore.GREEN}║     ██████╔╝██║   ██║███████╗   ██║   ██║  ███╗██████╔╝█████╗  ██║   ██║██║            ║
{Fore.GREEN}║     ██╔═══╝ ██║   ██║╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  ██║   ██║██║            ║
{Fore.GREEN}║     ██║     ╚██████╔╝███████║   ██║   ╚██████╔╝██║  ██║███████╗╚██████╔╝███████╗       ║
{Fore.GREEN}║     ╚═╝      ╚═════╝ ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚══════╝       ║
{Fore.GREEN}║                                                                                            ║
{Fore.CYAN}║                            🚀 ULTIMATE ENTERPRISE SYSTEM v5.0.0 🚀                        ║
{Fore.CYAN}║                          ═══════════════════════════════════════════                       ║
{Fore.YELLOW}║                          🎯 99% PERFORMANCE | ⚡ REAL-TIME | 🔒 SECURE                   ║
{Fore.CYAN}╚════════════════════════════════════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}
"""
        print(logo)
        
        # Status Bar - Real-time
        status_color = Fore.GREEN if self.current_db_url else Fore.YELLOW
        db_status = f"{status_color}●{Style.RESET_ALL}" if self.current_db_url else f"{Fore.RED}○{Style.RESET_ALL}"
        
        menu_indicator = f"{Fore.CYAN}【{'●' if self.menu_section == 1 else '○'} DATABASE │ {'○' if self.menu_section == 1 else '●'} DEPLOYMENT】{Style.RESET_ALL}"
        
        print(f"{Fore.WHITE}┌─{'─' * 98}─┐{Style.RESET_ALL}")
        print(f"{Fore.WHITE}│{Style.RESET_ALL}  {db_status} PostgreSQL  |  📊 URLs: {len(self.url_manager.urls)}  |  📦 Deployments: {len(self.deployment_manager.deployments)}  ")
        if self.current_db_url:
            print(f"  |  🔗 Active: {self.current_db_url.database}@{self.current_db_url.host}  ")
        print(f"  |  ⚙️  Mode: {'🚀 Performance' if self.performance_mode else '🔧 Standard'}  ")
        print(f"  |  🎯 Menu: {menu_indicator}  ")
        print(f"{Fore.WHITE}│{Style.RESET_ALL}")
        print(f"{Fore.WHITE}└─{'─' * 98}─┘{Style.RESET_ALL}")
        print()
    
    def print_menu_database(self):
        """1-QISM: DATABASE MANAGEMENT MENU"""
        menu_width = 98
        
        print(f"{Fore.CYAN}╔{'═' * menu_width}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 20}📁 SECTION 1: DATABASE MANAGEMENT SYSTEM{' ' * 20}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╠{'═' * menu_width}╣{Style.RESET_ALL}")
        
        # LEFT PANEL - DATABASE & USERS
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.GREEN}📁 DATABASE OPERATIONS{Style.RESET_ALL}          {' ' * 34} {Fore.GREEN}👥 USER OPERATIONS{Style.RESET_ALL}          {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [01] ➕ Create Database            │  [10] 👤 Create User                 {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [02] 🗑️  Drop Database              │  [11] 🗑️  Drop User                   {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [03] 📋 List Databases             │  [12] 📋 List Users                  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [04] 📊 Database Sizes             │  [13] ✏️  Modify User                 {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [05] 💾 Backup Database            │  [14] 🔐 Change Password             {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [06] 🔄 Restore Database           │  [15] 📊 User Activity               {Fore.CYAN}║{Style.RESET_ALL}")
        print()
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.YELLOW}🔐 PRIVILEGES & ROLES{Style.RESET_ALL}          {' ' * 34} {Fore.MAGENTA}📦 DATA OPERATIONS{Style.RESET_ALL}             {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [07] 🔑 Grant Privileges           │  [16] 📝 Insert Single Record        {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [08] 🔒 Revoke Privileges          │  [17] 📦 Bulk Insert                {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [09] 👥 User Privileges            │  [18] 📁 Import CSV/JSON            {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [19] 🎭 Create Role                │  [20] 🔗 Assign Role                {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [21] 🔓 Revoke Role                │  [22] 📋 List Roles                 {Fore.CYAN}║{Style.RESET_ALL}")
        print()
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.BLUE}🔗 URL CONNECTION{Style.RESET_ALL}              {' ' * 34} {Fore.CYAN}⚙️  UTILITIES{Style.RESET_ALL}                   {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [23] ➕ Add Database URL           │  [31] 📜 View Logs                  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [24] 📋 List URLs                  │  [32] ⚙️  Configure                 {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [25] 🎯 Select URL                 │  [33] 🧹 Cleanup                   {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [26] ✏️  Edit URL                  │  [34] 📈 Performance Test           {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [27] 🗑️  Remove URL                │  [35] 📖 Help/Manual               {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [28] 🔍 Test Connection            │                                      {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [29] 🎲 Generate URL               │                                      {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [30] 🏷️  Add Tags                  │                                      {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * menu_width}╝{Style.RESET_ALL}")
    
    def print_menu_deployment(self):
        """2-QISM: DEPLOYMENT & MONITORING MENU"""
        menu_width = 98
        
        print(f"{Fore.CYAN}╔{'═' * menu_width}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 18}🚀 SECTION 2: DEPLOYMENT & MONITORING SYSTEM{' ' * 18}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╠{'═' * menu_width}╣{Style.RESET_ALL}")
        
        # LEFT PANEL - DEPLOYMENT
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.GREEN}🚀 DEPLOYMENT MANAGEMENT{Style.RESET_ALL}        {' ' * 32} {Fore.YELLOW}📊 MONITORING{Style.RESET_ALL}                {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [51] 🚀 Create Deployment          │  [61] 📊 Start Monitoring            {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [52] 📋 List Deployments           │  [62] ⏹️  Stop Monitoring             {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [53] 🎯 Select Deployment          │  [63] 📈 Real-time Metrics           {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [54] 🗑️  Remove Deployment         │  [64] 🔌 Active Connections          {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [55] 🔍 Test Deployment            │  [65] 🐌 Slow Queries                {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [56] 📜 Deployment Logs           │  [66] 💾 Database Sizes              {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [57] 📤 Export Config             │  [67] 📊 Table Sizes                {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [58] 📥 Import Config             │  [68] ⚡ Cache Analysis              {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [59] 🔄 Scale Deployment          │  [69] 🔒 Lock Conflicts             {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [60] 📦 Deployment Status         │  [70] 🔄 Replication Status          {Fore.CYAN}║{Style.RESET_ALL}")
        print()
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.MAGENTA}🐍 PYTHON GENERATORS{Style.RESET_ALL}           {' ' * 32} {Fore.RED}⚠️  ALERTS{Style.RESET_ALL}                    {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [71] 🏠 Local Client Generator     │  [81] ⚠️  View Alerts                {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [72] ☁️  Remote Client Generator   │  [82] 🔔 Alert Settings              {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [73] 🐳 Docker Compose Generator   │  [83] 📊 Metrics History             {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [74] ☸️  Kubernetes Generator      │  [84] 📈 Performance Report          {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [75] 🔧 Environment Generator     │  [85] 🏥 Health Check                {Fore.CYAN}║{Style.RESET_ALL}")
        print()
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {Fore.BLUE}🔄 BACKUP & RESTORE{Style.RESET_ALL}            {' ' * 32} {Fore.CYAN}⚙️  SYSTEM{Style.RESET_ALL}                     {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [91] 💾 Scheduled Backup           │  [96] 🎨 Toggle Theme                {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [92] 🔄 Point-in-time Recovery    │  [97] 🚀 Performance Mode            {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [93] 📋 List Backups              │  [98] 🔄 Switch Menu Section         {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [94] 🧹 Clean Old Backups         │  [99] 🔄 Reload State                {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  [95] 📊 Backup Status             │  [00] 🚪 Exit                      {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Style.RESET_ALL}  {'─' * 96}  {Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * menu_width}╝{Style.RESET_ALL}")
    
    def print_quick_actions(self):
        """Tezkor amallar paneli"""
        print(f"{Fore.CYAN}┌─{'─' * 98}─┐{Style.RESET_ALL}")
        print(f"{Fore.CYAN}│{Style.RESET_ALL}  {Fore.YELLOW}⚡ QUICK ACTIONS:{Style.RESET_ALL}  ")
        if self.menu_section == 1:
            print(f"  [01] Create DB  [10] Create User  [07] Grant Privs  [16] Insert  [29] Generate URL  ")
        else:
            print(f"  [51] Deploy  [61] Monitor  [71] Python Client  [91] Backup  [85] Health Check  ")
        print(f"{Fore.CYAN}│{Style.RESET_ALL}")
        print(f"{Fore.CYAN}└─{'─' * 98}─┘{Style.RESET_ALL}")
    
    def run(self):
        """Asosiy sikl"""
        try:
            while self.running:
                self.print_ultimate_header()
                
                if self.menu_section == 1:
                    self.print_menu_database()
                else:
                    self.print_menu_deployment()
                
                self.print_quick_actions()
                
                choice = input(f"\n{Fore.YELLOW}┌─[ {Fore.GREEN}POSTGRES-ULTIMATE{Fore.YELLOW} ]{Style.RESET_ALL}\n{Fore.YELLOW}└──╼ $ {Style.RESET_ALL}")
                
                # Handle menu switching
                if choice == '98':
                    self.menu_section = 2 if self.menu_section == 1 else 1
                    self.save_state()
                    continue
                
                # Process choice based on current menu section
                if self.menu_section == 1:
                    self._process_database_choice(choice)
                else:
                    self._process_deployment_choice(choice)
        
        except KeyboardInterrupt:
            self._exit()
        except Exception as e:
            logger.critical(f"System error: {e}")
            self._exit()
    
    def _process_database_choice(self, choice: str):
        """1-QISM: Database management choices"""
        
        # DATABASE OPERATIONS
        if choice == '1':
            self._create_database_ui()
        elif choice == '2':
            self._drop_database_ui()
        elif choice == '3':
            self._list_databases_ui()
        elif choice == '4':
            self._database_sizes_ui()
        elif choice == '5':
            self._backup_database_ui()
        elif choice == '6':
            self._restore_database_ui()
        
        # USER OPERATIONS
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
            self._user_activity_ui()
        
        # PRIVILEGES
        elif choice == '7':
            self._grant_privileges_ui()
        elif choice == '8':
            self._revoke_privileges_ui()
        elif choice == '9':
            self._user_privileges_ui()
        
        # ROLES
        elif choice == '19':
            self._create_role_ui()
        elif choice == '20':
            self._assign_role_ui()
        elif choice == '21':
            self._revoke_role_ui()
        elif choice == '22':
            self._list_roles_ui()
        
        # DATA OPERATIONS
        elif choice == '16':
            self._insert_single_ui()
        elif choice == '17':
            self._insert_bulk_ui()
        elif choice == '18':
            self._import_file_ui()
        
        # URL MANAGEMENT
        elif choice == '23':
            self._add_url_ui()
        elif choice == '24':
            self._list_urls_ui()
        elif choice == '25':
            self._select_url_ui()
        elif choice == '26':
            self._edit_url_ui()
        elif choice == '27':
            self._remove_url_ui()
        elif choice == '28':
            self._test_url_ui()
        elif choice == '29':
            self._generate_url_ui()
        elif choice == '30':
            self._add_tags_ui()
        
        # UTILITIES
        elif choice == '31':
            self._view_logs_ui()
        elif choice == '32':
            self._configure_ui()
        elif choice == '33':
            self._cleanup_ui()
        elif choice == '34':
            self._performance_test_ui()
        elif choice == '35':
            self._help_ui()
        
        elif choice == '0' or choice == '00':
            self._exit()
        
        else:
            logger.warning(f"Invalid choice: {choice}")
            time.sleep(1)
    
    def _process_deployment_choice(self, choice: str):
        """2-QISM: Deployment & Monitoring choices"""
        
        # DEPLOYMENT MANAGEMENT
        if choice == '51':
            self._create_deployment_ui()
        elif choice == '52':
            self._list_deployments_ui()
        elif choice == '53':
            self._select_deployment_ui()
        elif choice == '54':
            self._remove_deployment_ui()
        elif choice == '55':
            self._test_deployment_ui()
        elif choice == '56':
            self._deployment_logs_ui()
        elif choice == '57':
            self._export_config_ui()
        elif choice == '58':
            self._import_config_ui()
        elif choice == '59':
            self._scale_deployment_ui()
        elif choice == '60':
            self._deployment_status_ui()
        
        # MONITORING
        elif choice == '61':
            self._start_monitoring_ui()
        elif choice == '62':
            self._stop_monitoring_ui()
        elif choice == '63':
            self._show_metrics_ui()
        elif choice == '64':
            self._show_connections_ui()
        elif choice == '65':
            self._show_slow_queries_ui()
        elif choice == '66':
            self._database_sizes_ui()
        elif choice == '67':
            self._table_sizes_ui()
        elif choice == '68':
            self._cache_analysis_ui()
        elif choice == '69':
            self._lock_conflicts_ui()
        elif choice == '70':
            self._replication_status_ui()
        
        # PYTHON GENERATORS
        elif choice == '71':
            self._generate_local_client_ui()
        elif choice == '72':
            self._generate_remote_client_ui()
        elif choice == '73':
            self._generate_docker_compose_ui()
        elif choice == '74':
            self._generate_kubernetes_ui()
        elif choice == '75':
            self._generate_env_file_ui()
        
        # ALERTS
        elif choice == '81':
            self._view_alerts_ui()
        elif choice == '82':
            self._alert_settings_ui()
        elif choice == '83':
            self._metrics_history_ui()
        elif choice == '84':
            self._performance_report_ui()
        elif choice == '85':
            self._health_check_ui()
        
        # BACKUP & RESTORE
        elif choice == '91':
            self._scheduled_backup_ui()
        elif choice == '92':
            self._pitr_ui()
        elif choice == '93':
            self._list_backups_ui()
        elif choice == '94':
            self._clean_backups_ui()
        elif choice == '95':
            self._backup_status_ui()
        
        # SYSTEM
        elif choice == '96':
            self._toggle_theme()
        elif choice == '97':
            self._toggle_performance_mode()
        elif choice == '98':
            self.menu_section = 1
            self.save_state()
        elif choice == '99':
            self.load_state()
        
        elif choice == '0' or choice == '00':
            self._exit()
        
        else:
            logger.warning(f"Invalid choice: {choice}")
            time.sleep(1)
    
    # ========================================================================
    # UI IMPLEMENTATIONS - DATABASE SECTION
    # ========================================================================
    
    def _create_database_ui(self):
        """Database yaratish UI"""
        if not self.current_pg_manager:
            logger.error("Please select a database URL first")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 60}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 20}📁 CREATE DATABASE{' ' * 21}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 60}╝{Style.RESET_ALL}")
        print()
        
        db_name = input(f"{Fore.GREEN}Database name: {Style.RESET_ALL}").strip()
        owner = input(f"{Fore.GREEN}Owner [postgres]: {Style.RESET_ALL}").strip() or "postgres"
        encoding = input(f"{Fore.GREEN}Encoding [UTF8]: {Style.RESET_ALL}").strip() or "UTF8"
        
        if self.current_pg_manager.create_database(db_name, owner, encoding):
            logger.success(f"Database '{db_name}' created successfully")
        else:
            logger.error(f"Failed to create database '{db_name}'")
        
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    def _create_user_ui(self):
        """User yaratish UI"""
        if not self.current_pg_manager:
            logger.error("Please select a database URL first")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 60}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 20}👤 CREATE USER{' ' * 24}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 60}╝{Style.RESET_ALL}")
        print()
        
        username = input(f"{Fore.GREEN}Username: {Style.RESET_ALL}").strip()
        
        print(f"\n{Fore.CYAN}Role Type:{Style.RESET_ALL}")
        for i, role in enumerate(UserRole, 1):
            print(f"  {i}. {role.value}")
        
        role_choice = input(f"\n{Fore.GREEN}Select role [5]: {Style.RESET_ALL}").strip()
        roles = list(UserRole)
        role = roles[int(role_choice) - 1] if role_choice.isdigit() and 1 <= int(role_choice) <= len(roles) else UserRole.READ_WRITE
        
        print(f"\n{Fore.YELLOW}Additional privileges:{Style.RESET_ALL}")
        superuser = input(f"Superuser? [y/N]: {Style.RESET_ALL}").strip().lower() == 'y'
        createdb = input(f"Create database? [y/N]: {Style.RESET_ALL}").strip().lower() == 'y'
        createrole = input(f"Create role? [y/N]: {Style.RESET_ALL}").strip().lower() == 'y'
        
        if self.current_pg_manager.create_user(
            username=username,
            superuser=superuser,
            createdb=createdb,
            createrole=createrole,
            role=role
        ):
            logger.success(f"User '{username}' created successfully")
        else:
            logger.error(f"Failed to create user '{username}'")
        
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    def _grant_privileges_ui(self):
        """Ruxsat berish UI"""
        if not self.current_pg_manager:
            logger.error("Please select a database URL first")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 60}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 18}🔐 GRANT PRIVILEGES{' ' * 19}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 60}╝{Style.RESET_ALL}")
        print()
        
        username = input(f"{Fore.GREEN}Username: {Style.RESET_ALL}").strip()
        
        print(f"\n{Fore.CYAN}Privileges (comma-separated):{Style.RESET_ALL}")
        print(f"  Options: SELECT, INSERT, UPDATE, DELETE, ALL")
        privileges_str = input(f"{Fore.GREEN}Privileges [ALL]: {Style.RESET_ALL}").strip()
        privileges = [p.strip().upper() for p in privileges_str.split(',')] if privileges_str else None
        
        db_name = input(f"{Fore.GREEN}Database name (Enter for current): {Style.RESET_ALL}").strip() or None
        
        if self.current_pg_manager.grant_privileges(username, db_name, privileges=privileges):
            logger.success(f"Privileges granted to '{username}'")
        else:
            logger.error(f"Failed to grant privileges to '{username}'")
        
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    def _insert_single_ui(self):
        """Single record insert UI"""
        if not self.current_pg_manager:
            logger.error("Please select a database URL first")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 60}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 18}📝 INSERT SINGLE RECORD{' ' * 17}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 60}╝{Style.RESET_ALL}")
        print()
        
        table = input(f"{Fore.GREEN}Table name: {Style.RESET_ALL}").strip()
        
        print(f"\n{Fore.CYAN}Enter data in JSON format:{Style.RESET_ALL}")
        print(f"  Example: {{\"name\": \"John\", \"age\": 30, \"email\": \"john@example.com\"}}")
        
        try:
            data_str = input(f"{Fore.GREEN}Data: {Style.RESET_ALL}").strip()
            data = json.loads(data_str)
            
            successful, _ = self.current_pg_manager.insert_data(table, data)
            
            if successful > 0:
                logger.success(f"Record inserted successfully")
            else:
                logger.error("Failed to insert record")
        except json.JSONDecodeError:
            logger.error("Invalid JSON format")
        except Exception as e:
            logger.error(f"Error: {e}")
        
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    # ========================================================================
    # UI IMPLEMENTATIONS - DEPLOYMENT SECTION
    # ========================================================================
    
    def _create_deployment_ui(self):
        """Deployment yaratish UI"""
        if not self.url_manager.urls:
            logger.error("Please create a database URL first")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 60}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 18}🚀 CREATE DEPLOYMENT{' ' * 19}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 60}╝{Style.RESET_ALL}")
        print()
        
        # List available URLs
        print(f"{Fore.CYAN}Available Database URLs:{Style.RESET_ALL}")
        for i, (name, url) in enumerate(self.url_manager.list_urls(), 1):
            print(f"  {i}. {name} - {url.to_string(hide_password=True)}")
        
        url_name = input(f"\n{Fore.GREEN}Select URL name: {Style.RESET_ALL}").strip()
        
        if url_name not in self.url_manager.urls:
            logger.error(f"URL '{url_name}' not found")
            input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
            return
        
        name = input(f"{Fore.GREEN}Deployment name: {Style.RESET_ALL}").strip()
        
        print(f"\n{Fore.CYAN}Environment:{Style.RESET_ALL}")
        for i, env in enumerate(EnvironmentType, 1):
            print(f"  {i}. {env.value}")
        env_choice = input(f"{Fore.GREEN}Select [1]: {Style.RESET_ALL}").strip() or '1'
        envs = list(EnvironmentType)
        environment = envs[int(env_choice) - 1] if env_choice.isdigit() and 1 <= int(env_choice) <= len(envs) else EnvironmentType.DEVELOPMENT
        
        print(f"\n{Fore.CYAN}Deployment Type:{Style.RESET_ALL}")
        for i, dep in enumerate(DeploymentType, 1):
            print(f"  {i}. {dep.value}")
        dep_choice = input(f"{Fore.GREEN}Select [1]: {Style.RESET_ALL}").strip() or '1'
        deps = list(DeploymentType)
        deployment_type = deps[int(dep_choice) - 1] if dep_choice.isdigit() and 1 <= int(dep_choice) <= len(deps) else DeploymentType.LOCAL
        
        print(f"\n{Fore.CYAN}Connection Mode:{Style.RESET_ALL}")
        for i, mode in enumerate(ConnectionMode, 1):
            print(f"  {i}. {mode.value}")
        mode_choice = input(f"{Fore.GREEN}Select [1]: {Style.RESET_ALL}").strip() or '1'
        modes = list(ConnectionMode)
        connection_mode = modes[int(mode_choice) - 1] if mode_choice.isdigit() and 1 <= int(mode_choice) <= len(modes) else ConnectionMode.DIRECT
        
        config = {
            'pool_size': input(f"{Fore.GREEN}Pool size [20]: {Style.RESET_ALL}").strip() or 20,
            'timeout': input(f"{Fore.GREEN}Timeout [30]: {Style.RESET_ALL}").strip() or 30,
            'ssl_enabled': input(f"{Fore.GREEN}SSL enabled? [Y/n]: {Style.RESET_ALL}").strip().lower() != 'n'
        }
        
        if self.deployment_manager.create_deployment(
            name=name,
            url_name=url_name,
            environment=environment,
            deployment_type=deployment_type,
            connection_mode=connection_mode,
            **config
        ):
            logger.success(f"Deployment '{name}' created successfully")
            
            # Create PostgreSQL manager for this deployment
            url = self.url_manager.get_url(url_name)
            self.current_pg_manager = PostgreSQLManager(url)
            self.current_db_url = url
            self.current_deployment = name
        else:
            logger.error(f"Failed to create deployment '{name}'")
        
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    def _start_monitoring_ui(self):
        """Monitoring boshlash UI"""
        if not self.current_pg_manager:
            logger.error("Please select a deployment first")
            input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
            return
        
        self.current_pg_manager.start_monitoring()
        logger.success("Monitoring started")
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    def _show_metrics_ui(self):
        """Real-time metrics UI"""
        if not self.current_pg_manager:
            logger.error("Please select a deployment first")
            input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 80}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 30}📈 REAL-TIME METRICS{' ' * 31}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 80}╝{Style.RESET_ALL}")
        print()
        
        try:
            metrics = self.current_pg_manager.get_metrics()
            
            # Connections
            print(f"{Fore.CYAN}🔌 CONNECTIONS:{Style.RESET_ALL}")
            print(f"  Total: {metrics['connections']['total_connections']}")
            print(f"  Active: {Fore.GREEN if metrics['connections']['active_connections'] < 50 else Fore.YELLOW}{metrics['connections']['active_connections']}{Style.RESET_ALL}")
            print(f"  Idle: {metrics['connections']['idle_connections']}")
            print(f"  Idle in TX: {Fore.RED if metrics['connections']['idle_in_transaction'] > 0 else Fore.GREEN}{metrics['connections']['idle_in_transaction']}{Style.RESET_ALL}")
            print()
            
            # Cache
            if metrics['cache']:
                print(f"{Fore.CYAN}💾 CACHE HIT RATIO:{Style.RESET_ALL}")
                cache_color = Fore.GREEN if metrics['cache']['cache_hit_ratio'] > 95 else Fore.YELLOW if metrics['cache']['cache_hit_ratio'] > 90 else Fore.RED
                print(f"  Table Cache: {cache_color}{metrics['cache']['cache_hit_ratio']:.1f}%{Style.RESET_ALL}")
                print(f"  Index Cache: {metrics['cache']['index_cache_ratio']:.1f}%")
                print()
            
            # Database
            print(f"{Fore.CYAN}💿 DATABASES:{Style.RESET_ALL}")
            print(f"  Count: {metrics['databases']['database_count']}")
            print(f"  Total Size: {metrics['databases']['total_size']}")
            print()
            
            # Indexes
            if metrics['indexes']:
                print(f"{Fore.CYAN}📊 INDEX USAGE:{Style.RESET_ALL}")
                print(f"  Index Scans: {metrics['indexes']['total_index_scans']}")
                print(f"  Index Usage Ratio: {metrics['indexes']['index_usage_ratio']:.1f}%")
                print()
            
            # Transactions
            if metrics['transactions']:
                print(f"{Fore.CYAN}🔄 TRANSACTIONS:{Style.RESET_ALL}")
                print(f"  Commit Ratio: {metrics['transactions']['commit_ratio']:.1f}%")
                print(f"  Rollbacks: {metrics['transactions']['xact_rollback']}")
                print()
            
            # Slow queries
            slow_queries = self.current_pg_manager.get_slow_queries()
            if slow_queries:
                print(f"{Fore.RED}🐌 SLOW QUERIES ({len(slow_queries)}):{Style.RESET_ALL}")
                for q in slow_queries[:5]:
                    print(f"  • {q['duration']}: {q['query'][:80]}...")
            
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
        
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    def _generate_local_client_ui(self):
        """Local Python client generator"""
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 60}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 15}🐍 LOCAL PYTHON CLIENT GENERATOR{' ' * 14}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 60}╝{Style.RESET_ALL}")
        print()
        
        filename = input(f"{Fore.GREEN}Output file [postgres_local_client.py]: {Style.RESET_ALL}").strip()
        if not filename:
            filename = "postgres_local_client.py"
        
        # Generate client code
        code = '''#!/usr/bin/env python3
"""
PostgreSQL Local Client - Generated by Ultimate System v5.0.0
"""
import os
import psycopg2
import psycopg2.pool
from psycopg2 import extras
from typing import Dict, List, Optional
from contextlib import contextmanager
import logging

class PostgreSQLClient:
    def __init__(self, host='localhost', port=5432, database=None,
                 user=None, password=None, min_conn=1, max_conn=10):
        self.params = {
            'host': host or os.environ.get('PGHOST', 'localhost'),
            'port': port or int(os.environ.get('PGPORT', 5432)),
            'dbname': database or os.environ.get('PGDATABASE'),
            'user': user or os.environ.get('PGUSER'),
            'password': password or os.environ.get('PGPASSWORD'),
            'connect_timeout': 10
        }
        self.pool = psycopg2.pool.SimpleConnectionPool(min_conn, max_conn, **self.params)
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('PostgreSQL-Client')
    
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
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return self.execute(query, tuple(data.values()), fetch=False)
    
    def select(self, table: str, columns: List[str] = None, 
               where: Dict = None, limit: int = None):
        cols = ', '.join(columns) if columns else '*'
        query = f"SELECT {cols} FROM {table}"
        params = []
        
        if where:
            conditions = ' AND '.join([f"{k} = %s" for k in where.keys()])
            query += f" WHERE {conditions}"
            params.extend(where.values())
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute(query, tuple(params) if params else None)
    
    def close(self):
        self.pool.closeall()

# Example usage
if __name__ == '__main__':
    db = PostgreSQLClient(
        host='localhost',
        port=5432,
        database='postgres',
        user='postgres',
        password='your_password'
    )
    
    try:
        result = db.execute("SELECT version()")
        print(f"Connected to: {result[0]['version']}")
    finally:
        db.close()
'''
        
        with open(filename, 'w') as f:
            f.write(code)
        
        os.chmod(filename, 0o755)
        logger.success(f"Local client generated: {filename}")
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    def _generate_remote_client_ui(self):
        """Remote Python client generator"""
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 60}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 14}☁️ REMOTE PYTHON CLIENT GENERATOR{' ' * 14}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 60}╝{Style.RESET_ALL}")
        print()
        
        filename = input(f"{Fore.GREEN}Output file [postgres_remote_client.py]: {Style.RESET_ALL}").strip()
        if not filename:
            filename = "postgres_remote_client.py"
        
        # Generate production client code
        code = '''#!/usr/bin/env python3
"""
PostgreSQL Remote/Production Client - Generated by Ultimate System v5.0.0
Features: SSL, Connection Pool, Retry Logic, Monitoring
"""
import os
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
from psycopg2 import pool, errors
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
from datetime import datetime
import logging
import urllib.parse
from functools import wraps

class ProductionConfig:
    POOL_MIN_SIZE = 5
    POOL_MAX_SIZE = 20
    CONNECTION_TIMEOUT = 30
    STATEMENT_TIMEOUT = 30000
    MAX_RETRIES = 3
    RETRY_DELAY = 1
    SSL_MODE = 'require'

class DatabaseURL:
    def __init__(self, url: str):
        parsed = urllib.parse.urlparse(url)
        self.host = parsed.hostname or 'localhost'
        self.port = parsed.port or 5432
        self.user = urllib.parse.unquote(parsed.username) if parsed.username else None
        self.password = urllib.parse.unquote(parsed.password) if parsed.password else None
        self.dbname = parsed.path.lstrip('/') if parsed.path else None
        self.params = dict(urllib.parse.parse_qsl(parsed.query))
        self.sslmode = self.params.get('sslmode', ProductionConfig.SSL_MODE)

class ProductionPostgreSQLClient:
    def __init__(self, database_url: str, app_name: str = None):
        self.db_url = DatabaseURL(database_url)
        self.app_name = app_name or 'ProductionApp'
        self.pool = None
        self.metrics = {'queries': 0, 'errors': 0, 'start_time': datetime.now()}
        self._create_pool()
        self._setup_logging()
    
    def _create_pool(self):
        conn_params = {
            'host': self.db_url.host,
            'port': self.db_url.port,
            'user': self.db_url.user,
            'password': self.db_url.password,
            'dbname': self.db_url.dbname,
            'sslmode': self.db_url.sslmode,
            'connect_timeout': ProductionConfig.CONNECTION_TIMEOUT,
            'application_name': self.app_name,
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5,
            'options': f'-c statement_timeout={ProductionConfig.STATEMENT_TIMEOUT}'
        }
        
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            ProductionConfig.POOL_MIN_SIZE,
            ProductionConfig.POOL_MAX_SIZE,
            **conn_params
        )
    
    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f'PostgreSQL-Production')
    
    def retry_on_failure(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            for attempt in range(ProductionConfig.MAX_RETRIES):
                try:
                    return func(self, *args, **kwargs)
                except (errors.OperationalError, errors.InterfaceError) as e:
                    if attempt == ProductionConfig.MAX_RETRIES - 1:
                        raise
                    time.sleep(ProductionConfig.RETRY_DELAY * (attempt + 1))
            return None
        return wrapper
    
    @contextmanager
    @retry_on_failure
    def get_cursor(self):
        conn = self.pool.getconn()
        try:
            yield conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.metrics['errors'] += 1
            raise
        finally:
            self.pool.putconn(conn)
    
    @retry_on_failure
    def execute(self, query: str, params: tuple = None, fetch: bool = True):
        self.metrics['queries'] += 1
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if fetch and cur.description else None
    
    def health_check(self) -> Dict:
        try:
            start = time.time()
            self.execute("SELECT 1", fetch=False)
            response_time = time.time() - start
            return {
                'status': 'healthy',
                'response_time': response_time,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {'status': 'unhealthy', 'error': str(e)}
    
    def close(self):
        if self.pool:
            self.pool.closeall()

# Example usage
if __name__ == '__main__':
    DATABASE_URL = os.environ.get('DATABASE_URL', 
        'postgresql://user:password@host:5432/db?sslmode=require')
    
    db = ProductionPostgreSQLClient(DATABASE_URL, app_name="MyApp")
    
    try:
        health = db.health_check()
        print(f"Health: {health['status']}")
        
        result = db.execute("SELECT version()")
        print(f"Version: {result[0]['version']}")
    finally:
        db.close()
'''
        
        with open(filename, 'w') as f:
            f.write(code)
        
        os.chmod(filename, 0o755)
        logger.success(f"Remote client generated: {filename}")
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    def _health_check_ui(self):
        """Health check UI"""
        if not self.current_pg_manager:
            logger.error("Please select a deployment first")
            input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
            return
        
        self.clear_screen()
        print(f"{Fore.CYAN}╔{'═' * 60}╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║{Fore.YELLOW}{' ' * 20}🏥 HEALTH CHECK{' ' * 23}{Fore.CYAN}║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚{'═' * 60}╝{Style.RESET_ALL}")
        print()
        
        try:
            # Test connection
            start = time.time()
            result = self.current_pg_manager.execute_query("SELECT 1")
            response_time = time.time() - start
            
            print(f"{Fore.GREEN}✅ Status: Healthy{Style.RESET_ALL}")
            print(f"  Response Time: {response_time*1000:.2f}ms")
            print()
            
            # Get version
            version = self.current_pg_manager.execute_query("SELECT version()")
            if version:
                print(f"  PostgreSQL: {version[0]['version'].split(',')[0]}")
            
            # Get uptime
            uptime = self.current_pg_manager.execute_query("""
                SELECT 
                    pg_postmaster_start_time() as start_time,
                    now() - pg_postmaster_start_time() as uptime
            """)
            if uptime:
                print(f"  Uptime: {uptime[0]['uptime']}")
            
            # Get connection stats
            stats = self.current_pg_manager.execute_query("""
                SELECT 
                    count(*) as total,
                    count(*) FILTER (WHERE state = 'active') as active,
                    count(*) FILTER (WHERE state = 'idle') as idle,
                    count(DISTINCT datname) as databases,
                    count(DISTINCT usename) as users
                FROM pg_stat_activity
            """)
            if stats:
                print()
                print(f"{Fore.CYAN}📊 Connection Stats:{Style.RESET_ALL}")
                print(f"  Total: {stats[0]['total']}")
                print(f"  Active: {stats[0]['active']}")
                print(f"  Databases: {stats[0]['databases']}")
                print(f"  Users: {stats[0]['users']}")
            
        except Exception as e:
            print(f"{Fore.RED}❌ Status: Unhealthy{Style.RESET_ALL}")
            print(f"  Error: {e}")
        
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")
    
    def _exit(self):
        """Dasturdan chiqish"""
        self.save_state()
        
        if self.current_pg_manager:
            self.current_pg_manager.close()
        
        print(f"\n{Fore.GREEN}✅ PostgreSQL Ultimate System terminated{Style.RESET_ALL}")
        print(f"{Fore.CYAN}   Thank you for using Enterprise Edition! 👋{Style.RESET_ALL}")
        self.running = False
    
    # Placeholder methods - to be implemented fully
    def _list_databases_ui(self): self._not_implemented()
    def _database_sizes_ui(self): self._not_implemented()
    def _backup_database_ui(self): self._not_implemented()
    def _restore_database_ui(self): self._not_implemented()
    def _drop_database_ui(self): self._not_implemented()
    def _list_users_ui(self): self._not_implemented()
    def _drop_user_ui(self): self._not_implemented()
    def _modify_user_ui(self): self._not_implemented()
    def _change_password_ui(self): self._not_implemented()
    def _user_activity_ui(self): self._not_implemented()
    def _revoke_privileges_ui(self): self._not_implemented()
    def _user_privileges_ui(self): self._not_implemented()
    def _create_role_ui(self): self._not_implemented()
    def _assign_role_ui(self): self._not_implemented()
    def _revoke_role_ui(self): self._not_implemented()
    def _list_roles_ui(self): self._not_implemented()
    def _insert_bulk_ui(self): self._not_implemented()
    def _import_file_ui(self): self._not_implemented()
    def _add_url_ui(self): self._not_implemented()
    def _list_urls_ui(self): self._not_implemented()
    def _select_url_ui(self): self._not_implemented()
    def _edit_url_ui(self): self._not_implemented()
    def _remove_url_ui(self): self._not_implemented()
    def _test_url_ui(self): self._not_implemented()
    def _generate_url_ui(self): self._not_implemented()
    def _add_tags_ui(self): self._not_implemented()
    def _view_logs_ui(self): self._not_implemented()
    def _configure_ui(self): self._not_implemented()
    def _cleanup_ui(self): self._not_implemented()
    def _performance_test_ui(self): self._not_implemented()
    def _help_ui(self): self._not_implemented()
    def _list_deployments_ui(self): self._not_implemented()
    def _select_deployment_ui(self): self._not_implemented()
    def _remove_deployment_ui(self): self._not_implemented()
    def _test_deployment_ui(self): self._not_implemented()
    def _deployment_logs_ui(self): self._not_implemented()
    def _export_config_ui(self): self._not_implemented()
    def _import_config_ui(self): self._not_implemented()
    def _scale_deployment_ui(self): self._not_implemented()
    def _deployment_status_ui(self): self._not_implemented()
    def _stop_monitoring_ui(self): self._not_implemented()
    def _show_connections_ui(self): self._not_implemented()
    def _show_slow_queries_ui(self): self._not_implemented()
    def _table_sizes_ui(self): self._not_implemented()
    def _cache_analysis_ui(self): self._not_implemented()
    def _lock_conflicts_ui(self): self._not_implemented()
    def _replication_status_ui(self): self._not_implemented()
    def _generate_docker_compose_ui(self): self._not_implemented()
    def _generate_kubernetes_ui(self): self._not_implemented()
    def _generate_env_file_ui(self): self._not_implemented()
    def _view_alerts_ui(self): self._not_implemented()
    def _alert_settings_ui(self): self._not_implemented()
    def _metrics_history_ui(self): self._not_implemented()
    def _performance_report_ui(self): self._not_implemented()
    def _scheduled_backup_ui(self): self._not_implemented()
    def _pitr_ui(self): self._not_implemented()
    def _list_backups_ui(self): self._not_implemented()
    def _clean_backups_ui(self): self._not_implemented()
    def _backup_status_ui(self): self._not_implemented()
    def _toggle_theme(self): self._not_implemented()
    def _toggle_performance_mode(self): self._not_implemented()
    
    def _not_implemented(self):
        """Not implemented yet"""
        logger.warning("This feature will be available in the next update")
        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Dasturni ishga tushirish"""
    
    # Root huquqini tekshirish
    if os.geteuid() != 0:
        print(f"{Fore.YELLOW}⚠️  Running without root privileges - some features may be limited{Style.RESET_ALL}")
        print(f"{Fore.CYAN}   For full functionality, run with: sudo python3 postgres_ultimate.py{Style.RESET_ALL}")
        print()
        time.sleep(2)
    
    try:
        # Signal handlers
        def signal_handler(sig, frame):
            print(f"\n{Fore.YELLOW}⚠️  System interrupted{Style.RESET_ALL}")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Cleanup on exit
        def cleanup():
            print(f"{Fore.GREEN}✅ Cleanup completed{Style.RESET_ALL}")
        
        atexit.register(cleanup)
        
        # Start UI
        ui = UltimateUI()
        ui.run()
        
    except Exception as e:
        logger.critical(f"System error: {e}")
        print(f"{Fore.RED}❌ Fatal error: {e}{Style.RESET_ALL}")
        sys.exit(1)

if __name__ == "__main__":
    main()