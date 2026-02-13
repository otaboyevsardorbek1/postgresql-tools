#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
POSTGRESQL ENTERPRISE FULL-STACK MANAGEMENT SYSTEM
Version: 4.0.0
Muallif: DevOps Team

██████╗  ██████╗ ███████╗████████╗ ██████╗ ██████╗ ███████╗
██╔══██╗██╔═══██╗██╔════╝╚══██╔══╝██╔════╝ ██╔══██╗██╔════╝
██████╔╝██║   ██║███████╗   ██║   ██║  ███╗██████╔╝█████╗  
██╔═══╝ ██║   ██║╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  
██║     ╚██████╔╝███████║   ██║   ╚██████╔╝██║  ██║███████╗
╚═╝      ╚═════╝ ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝

📊 DATABASE URL MANAGEMENT | LOCAL & REMOTE DEPLOYMENT | PYTHON EXAMPLES
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
import urllib.parse
from typing import Dict, List, Tuple, Optional, Any, Union
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from prettytable import PrettyTable
from colorama import init, Fore, Back, Style
import getpass
import re
import requests
import socket
import ssl

# Colorama ni ishga tushurish
init(autoreset=True)

# ============================================================================
# KONFIGURATSIYA VA ENUMLAR
# ============================================================================

class DeploymentType(Enum):
    """Deployment turlari"""
    LOCAL = "local"
    REMOTE = "remote"
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    CLOUD = "cloud"

class ConnectionMode(Enum):
    """Ulanish rejimlari"""
    DIRECT = "direct"
    POOL = "pool"
    SSL = "ssl"
    SSH_TUNNEL = "ssh_tunnel"

class EnvironmentType(Enum):
    """Muhit turlari"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"

@dataclass
class DatabaseURL:
    """
    PostgreSQL Database URL management
    Format: postgresql://user:password@host:port/database?sslmode=require
    """
    scheme: str = "postgresql"
    username: str = None
    password: str = None
    host: str = "localhost"
    port: int = 5432
    database: str = None
    params: Dict[str, str] = field(default_factory=dict)
    
    @classmethod
    def from_string(cls, url: str) -> 'DatabaseURL':
        """URL string dan DatabaseURL obyekti yaratish"""
        parsed = urllib.parse.urlparse(url)
        
        # Scheme ni tekshirish
        if not parsed.scheme or parsed.scheme not in ['postgresql', 'postgres']:
            raise ValueError(f"Noto'g'ri scheme: {parsed.scheme}. 'postgresql' yoki 'postgres' bo'lishi kerak")
        
        # Ma'lumotlarni ajratib olish
        username = None
        password = None
        if parsed.username:
            username = urllib.parse.unquote(parsed.username)
        if parsed.password:
            password = urllib.parse.unquote(parsed.password)
        
        # Port ni aniqlash
        port = parsed.port or 5432
        
        # Query params
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
        
        # Database nomi
        db_path = f"/{self.database}" if self.database else ""
        
        # Query params
        query = ""
        if self.params:
            query = "?" + urllib.parse.urlencode(self.params)
        
        return f"{self.scheme}://{auth}{self.host}:{self.port}{db_path}{query}"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Psycopg2 uchun connection parametrlari"""
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
        
        # SSL sozlamalari
        sslmode = self.params.get('sslmode', 'prefer')
        if sslmode:
            params['sslmode'] = sslmode
        
        # SSL sertifikatlar
        if 'sslrootcert' in self.params:
            params['sslrootcert'] = self.params['sslrootcert']
        if 'sslcert' in self.params:
            params['sslcert'] = self.params['sslcert']
        if 'sslkey' in self.params:
            params['sslkey'] = self.params['sslkey']
        
        return params

@dataclass
class DeploymentConfig:
    """Deployment konfiguratsiyasi"""
    name: str
    environment: EnvironmentType
    deployment_type: DeploymentType
    connection_mode: ConnectionMode
    database_url: DatabaseURL
    pool_size: int = 20
    max_overflow: int = 10
    timeout: int = 30
    ssl_enabled: bool = True
    backup_enabled: bool = True
    monitoring_enabled: bool = True
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# LOGGER VA KONFIGURATSIYA
# ============================================================================

@dataclass
class Config:
    """Global konfiguratsiya"""
    APP_NAME: str = "PostgreSQL Enterprise Manager"
    VERSION: str = "4.0.0"
    LOG_DIR: str = "/var/log/pg_enterprise"
    LOG_FILE: str = f"/var/log/pg_enterprise/pg_manager_{datetime.datetime.now().strftime('%Y%m%d')}.log"
    CONFIG_DIR: str = "/etc/pg_enterprise"
    BACKUP_DIR: str = "/var/backups/postgresql"
    DATABASE_URLS_FILE: str = "/etc/pg_enterprise/database_urls.json"
    DEPLOYMENTS_FILE: str = "/etc/pg_enterprise/deployments.json"
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

class Logger:
    """Professional logging tizimi"""
    
    def __init__(self):
        os.makedirs(config.LOG_DIR, exist_ok=True)
        os.makedirs(config.CONFIG_DIR, exist_ok=True)
        os.makedirs(config.BACKUP_DIR, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(config.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('PostgreSQL_Enterprise')
    
    def info(self, message):
        self.logger.info(f"{Fore.CYAN}ℹ {message}{Style.RESET_ALL}")
    
    def success(self, message):
        self.logger.info(f"{Fore.GREEN}✅ {message}{Style.RESET_ALL}")
    
    def warning(self, message):
        self.logger.warning(f"{Fore.YELLOW}⚠ {message}{Style.RESET_ALL}")
    
    def error(self, message):
        self.logger.error(f"{Fore.RED}❌ {message}{Style.RESET_ALL}")
    
    def debug(self, message):
        self.logger.debug(f"{Fore.MAGENTA}🔍 {message}{Style.RESET_ALL}")

logger = Logger()

# ============================================================================
# DATABASE URL MANAGER
# ============================================================================

class DatabaseURLManager:
    """Database URL larni boshqarish tizimi"""
    
    def __init__(self):
        self.urls: Dict[str, DatabaseURL] = {}
        self.load_urls()
    
    def load_urls(self):
        """Saqlangan URL larni yuklash"""
        if os.path.exists(config.DATABASE_URLS_FILE):
            try:
                with open(config.DATABASE_URLS_FILE, 'r') as f:
                    data = json.load(f)
                    for name, url_str in data.items():
                        self.urls[name] = DatabaseURL.from_string(url_str)
                logger.success(f"{len(self.urls)} ta database URL yuklandi")
            except Exception as e:
                logger.error(f"URL larni yuklashda xato: {e}")
    
    def save_urls(self):
        """URL larni saqlash"""
        try:
            data = {name: url.to_string() for name, url in self.urls.items()}
            with open(config.DATABASE_URLS_FILE, 'w') as f:
                json.dump(data, f, indent=2)
            logger.success("Database URL lar saqlandi")
        except Exception as e:
            logger.error(f"URL larni saqlashda xato: {e}")
    
    def add_url(self, name: str, url: DatabaseURL) -> bool:
        """Yangi URL qo'shish"""
        if name in self.urls:
            logger.warning(f"'{name}' nomli URL allaqachon mavjud")
            return False
        self.urls[name] = url
        self.save_urls()
        logger.success(f"URL '{name}' qo'shildi: {url.to_string(hide_password=True)}")
        return True
    
    def remove_url(self, name: str) -> bool:
        """URL o'chirish"""
        if name not in self.urls:
            logger.error(f"'{name}' nomli URL topilmadi")
            return False
        del self.urls[name]
        self.save_urls()
        logger.success(f"URL '{name}' o'chirildi")
        return True
    
    def get_url(self, name: str) -> Optional[DatabaseURL]:
        """URL ni nomi bo'yicha olish"""
        return self.urls.get(name)
    
    def test_connection(self, url: DatabaseURL) -> Tuple[bool, str]:
        """Database ulanishni tekshirish"""
        try:
            conn = psycopg2.connect(**url.get_connection_params())
            conn.close()
            return True, "Ulanish muvaffaqiyatli"
        except Exception as e:
            return False, str(e)
    
    def generate_url(self, **kwargs) -> DatabaseURL:
        """Yangi URL generatsiya qilish"""
        return DatabaseURL(**kwargs)

# ============================================================================
# DEPLOYMENT MANAGER
# ============================================================================

class DeploymentManager:
    """Deployment larni boshqarish tizimi"""
    
    def __init__(self):
        self.deployments: Dict[str, DeploymentConfig] = {}
        self.load_deployments()
    
    def load_deployments(self):
        """Deployment larni yuklash"""
        if os.path.exists(config.DEPLOYMENTS_FILE):
            try:
                with open(config.DEPLOYMENTS_FILE, 'r') as f:
                    data = json.load(f)
                    for name, cfg in data.items():
                        url = DatabaseURL.from_string(cfg['database_url'])
                        self.deployments[name] = DeploymentConfig(
                            name=name,
                            environment=EnvironmentType(cfg['environment']),
                            deployment_type=DeploymentType(cfg['deployment_type']),
                            connection_mode=ConnectionMode(cfg['connection_mode']),
                            database_url=url,
                            pool_size=cfg.get('pool_size', 20),
                            max_overflow=cfg.get('max_overflow', 10),
                            timeout=cfg.get('timeout', 30),
                            ssl_enabled=cfg.get('ssl_enabled', True),
                            backup_enabled=cfg.get('backup_enabled', True),
                            monitoring_enabled=cfg.get('monitoring_enabled', True),
                            metadata=cfg.get('metadata', {})
                        )
                logger.success(f"{len(self.deployments)} ta deployment yuklandi")
            except Exception as e:
                logger.error(f"Deployment larni yuklashda xato: {e}")
    
    def save_deployments(self):
        """Deployment larni saqlash"""
        try:
            data = {}
            for name, dep in self.deployments.items():
                data[name] = {
                    'name': dep.name,
                    'environment': dep.environment.value,
                    'deployment_type': dep.deployment_type.value,
                    'connection_mode': dep.connection_mode.value,
                    'database_url': dep.database_url.to_string(),
                    'pool_size': dep.pool_size,
                    'max_overflow': dep.max_overflow,
                    'timeout': dep.timeout,
                    'ssl_enabled': dep.ssl_enabled,
                    'backup_enabled': dep.backup_enabled,
                    'monitoring_enabled': dep.monitoring_enabled,
                    'metadata': dep.metadata
                }
            with open(config.DEPLOYMENTS_FILE, 'w') as f:
                json.dump(data, f, indent=2)
            logger.success("Deployment lar saqlandi")
        except Exception as e:
            logger.error(f"Deployment larni saqlashda xato: {e}")
    
    def create_deployment(self, name: str, environment: EnvironmentType,
                         deployment_type: DeploymentType, 
                         connection_mode: ConnectionMode,
                         database_url: DatabaseURL, **kwargs) -> bool:
        """Yangi deployment yaratish"""
        if name in self.deployments:
            logger.warning(f"'{name}' nomli deployment allaqachon mavjud")
            return False
        
        deployment = DeploymentConfig(
            name=name,
            environment=environment,
            deployment_type=deployment_type,
            connection_mode=connection_mode,
            database_url=database_url,
            **kwargs
        )
        
        self.deployments[name] = deployment
        self.save_deployments()
        logger.success(f"Deployment '{name}' yaratildi")
        return True
    
    def remove_deployment(self, name: str) -> bool:
        """Deployment o'chirish"""
        if name not in self.deployments:
            logger.error(f"'{name}' nomli deployment topilmadi")
            return False
        del self.deployments[name]
        self.save_deployments()
        logger.success(f"Deployment '{name}' o'chirildi")
        return True
    
    def get_deployment(self, name: str) -> Optional[DeploymentConfig]:
        """Deployment ni nomi bo'yicha olish"""
        return self.deployments.get(name)
    
    def list_by_environment(self, environment: EnvironmentType) -> List[DeploymentConfig]:
        """Muhit bo'yicha deployment larni ro'yxatlash"""
        return [d for d in self.deployments.values() if d.environment == environment]

# ============================================================================
# DATA INSERTION MANAGER
# ============================================================================

class DataInsertionManager:
    """Database ga ma'lumot yuborish tizimi"""
    
    def __init__(self, database_url: DatabaseURL):
        self.database_url = database_url
        self.connection = None
        self.stats = {
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
    
    @contextmanager
    def get_connection(self):
        """Database ulanish"""
        conn = None
        try:
            conn = psycopg2.connect(**self.database_url.get_connection_params())
            yield conn
        except Exception as e:
            logger.error(f"Ulanish xatosi: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def insert_single(self, table: str, data: Dict[str, Any]) -> bool:
        """Bitta qator qo'shish"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    columns = ', '.join(data.keys())
                    placeholders = ', '.join(['%s'] * len(data))
                    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                    cur.execute(query, list(data.values()))
                    conn.commit()
                    self.stats['inserts'] += 1
                    return True
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Ma'lumot qo'shishda xato: {e}")
            return False
    
    def insert_bulk(self, table: str, data: List[Dict[str, Any]], 
                   batch_size: int = 1000) -> Tuple[int, int]:
        """Ko'p qatorlarni qo'shish"""
        successful = 0
        failed = 0
        
        if not data:
            return 0, 0
        
        columns = data[0].keys()
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        values_list = []
                        for row in batch:
                            values_list.append(tuple(row[col] for col in columns))
                        
                        placeholders = ','.join(['%s'] * len(columns))
                        query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
                        
                        try:
                            cur.executemany(query, values_list)
                            conn.commit()
                            successful += len(batch)
                            self.stats['inserts'] += len(batch)
                        except Exception as e:
                            conn.rollback()
                            failed += len(batch)
                            self.stats['errors'] += len(batch)
                            logger.error(f"Batch xatosi: {e}")
        
        except Exception as e:
            logger.error(f"Bulk insert xatosi: {e}")
        
        return successful, failed
    
    def insert_from_csv(self, table: str, csv_file: str, delimiter: str = ',') -> Tuple[int, int]:
        """CSV fayldan ma'lumot qo'shish"""
        import csv
        
        successful = 0
        failed = 0
        
        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                data = list(reader)
                
                # Ma'lumot turlarini aniqlash
                for row in data:
                    for key, value in row.items():
                        # Raqamlarni o'zgartirish
                        if value.replace('.', '').replace('-', '').isdigit():
                            if '.' in value:
                                row[key] = float(value)
                            else:
                                row[key] = int(value)
                
                successful, failed = self.insert_bulk(table, data)
                
        except Exception as e:
            logger.error(f"CSV import xatosi: {e}")
        
        return successful, failed
    
    def insert_from_json(self, table: str, json_file: str) -> Tuple[int, int]:
        """JSON fayldan ma'lumot qo'shish"""
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                
                if isinstance(data, dict):
                    data = [data]  # Single object
                
                return self.insert_bulk(table, data)
                
        except Exception as e:
            logger.error(f"JSON import xatosi: {e}")
            return 0, 0
    
    def update_data(self, table: str, data: Dict[str, Any], 
                   condition: Dict[str, Any]) -> bool:
        """Ma'lumot yangilash"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
                    where_clause = ' AND '.join([f"{k} = %s" for k in condition.keys()])
                    
                    query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
                    
                    params = list(data.values()) + list(condition.values())
                    cur.execute(query, params)
                    conn.commit()
                    
                    self.stats['updates'] += cur.rowcount
                    return True
                    
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Ma'lumot yangilashda xato: {e}")
            return False
    
    def delete_data(self, table: str, condition: Dict[str, Any]) -> bool:
        """Ma'lumot o'chirish"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    where_clause = ' AND '.join([f"{k} = %s" for k in condition.keys()])
                    query = f"DELETE FROM {table} WHERE {where_clause}"
                    
                    cur.execute(query, list(condition.values()))
                    conn.commit()
                    
                    self.stats['deletes'] += cur.rowcount
                    return True
                    
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Ma'lumot o'chirishda xato: {e}")
            return False
    
    def execute_query(self, query: str, params: tuple = None) -> Optional[List[Dict]]:
        """Ixtiyoriy SQL query bajarish"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    
                    if cur.description:  # SELECT query
                        columns = [desc[0] for desc in cur.description]
                        results = []
                        for row in cur.fetchall():
                            results.append(dict(zip(columns, row)))
                        return results
                    else:
                        conn.commit()
                        return None
                        
        except Exception as e:
            logger.error(f"Query xatosi: {e}")
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Statistika ma'lumotlarini olish"""
        return self.stats

# ============================================================================
# PYTHON CLIENT EXAMPLES - LOCAL VERSION
# ============================================================================

PYTHON_LOCAL_CLIENT = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PostgreSQL LOCAL Client Example
Muallif: DevOps Team
Version: 1.0.0
Environment: Development/Local
"""

import os
import json
import psycopg2
import psycopg2.pool
from psycopg2 import extras
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import logging
import urllib.parse

# ----------------------------------------------------------------------------
# DATABASE URL MANAGEMENT
# ----------------------------------------------------------------------------

@dataclass
class DatabaseURL:
    """Database URL parser"""
    scheme: str = "postgresql"
    username: str = None
    password: str = None
    host: str = "localhost"
    port: int = 5432
    database: str = None
    params: Dict[str, str] = None
    
    @classmethod
    def from_string(cls, url: str) -> 'DatabaseURL':
        parsed = urllib.parse.urlparse(url)
        
        username = None
        password = None
        if parsed.username:
            username = urllib.parse.unquote(parsed.username)
        if parsed.password:
            password = urllib.parse.unquote(parsed.password)
        
        params = dict(urllib.parse.parse_qsl(parsed.query))
        
        return cls(
            scheme=parsed.scheme,
            username=username,
            password=password,
            host=parsed.hostname or 'localhost',
            port=parsed.port or 5432,
            database=parsed.path.lstrip('/') if parsed.path else None,
            params=params
        )
    
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
        
        sslmode = self.params.get('sslmode', 'prefer') if self.params else 'prefer'
        params['sslmode'] = sslmode
        
        return params

# ----------------------------------------------------------------------------
# DATABASE CLIENT
# ----------------------------------------------------------------------------

class PostgreSQLClient:
    """
    PostgreSQL LOCAL Client
    - Local development uchun
    - Connection pooling
    - Automatic retry
    - Error handling
    """
    
    def __init__(self, database_url: str = None, **kwargs):
        """
        Initialize database client
        
        Args:
            database_url: PostgreSQL URL
            **kwargs: Individual connection parameters
        """
        if database_url:
            self.db_url = DatabaseURL.from_string(database_url)
        else:
            self.db_url = DatabaseURL(
                host=kwargs.get('host', 'localhost'),
                port=kwargs.get('port', 5432),
                username=kwargs.get('user', os.environ.get('PGUSER', 'postgres')),
                password=kwargs.get('password', os.environ.get('PGPASSWORD', '')),
                database=kwargs.get('database', kwargs.get('dbname', os.environ.get('PGDATABASE', 'postgres')))
            )
        
        self.pool = None
        self.max_retries = 3
        self.setup_logging()
        self.create_pool()
    
    def setup_logging(self):
        """Logging sozlash"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('PostgreSQL-Local')
    
    def create_pool(self, min_conn: int = 1, max_conn: int = 10):
        """Connection pool yaratish"""
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                min_conn,
                max_conn,
                **self.db_url.get_connection_params()
            )
            self.logger.info(f"Connection pool yaratildi: {min_conn}-{max_conn}")
        except Exception as e:
            self.logger.error(f"Pool yaratishda xato: {e}")
            raise
    
    @contextmanager
    def get_cursor(self, cursor_factory=extras.RealDictCursor):
        """Cursor olish"""
        conn = None
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor(cursor_factory=cursor_factory)
            yield cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Cursor xatosi: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.putconn(conn)
    
    def execute(self, query: str, params: tuple = None, fetch: bool = True):
        """Query bajarish"""
        for attempt in range(self.max_retries):
            try:
                with self.get_cursor() as cursor:
                    cursor.execute(query, params)
                    if fetch and cursor.description:
                        return cursor.fetchall()
                    return None
            except Exception as e:
                self.logger.warning(f"Urinish {attempt + 1} xato: {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(0.5 * (attempt + 1))
    
    def insert(self, table: str, data: Dict[str, Any]) -> bool:
        """Ma'lumot qo'shish"""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        try:
            self.execute(query, tuple(data.values()), fetch=False)
            self.logger.info(f"Ma'lumot qo'shildi: {table}")
            return True
        except Exception as e:
            self.logger.error(f"Insert xatosi: {e}")
            return False
    
    def select(self, table: str, columns: List[str] = None, 
              where: Dict[str, Any] = None, limit: int = None):
        """Ma'lumot o'qish"""
        cols = ', '.join(columns) if columns else '*'
        query = f"SELECT {cols} FROM {table}"
        params = []
        
        if where:
            conditions = []
            for key, value in where.items():
                conditions.append(f"{key} = %s")
                params.append(value)
            query += " WHERE " + " AND ".join(conditions)
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute(query, tuple(params) if params else None)
    
    def update(self, table: str, data: Dict[str, Any], 
              where: Dict[str, Any]) -> int:
        """Ma'lumot yangilash"""
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = %s" for k in where.keys()])
        
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = tuple(list(data.values()) + list(where.values()))
        
        try:
            self.execute(query, params, fetch=False)
            self.logger.info(f"Ma'lumot yangilandi: {table}")
            return True
        except Exception as e:
            self.logger.error(f"Update xatosi: {e}")
            return False
    
    def delete(self, table: str, where: Dict[str, Any]) -> bool:
        """Ma'lumot o'chirish"""
        where_clause = ' AND '.join([f"{k} = %s" for k in where.keys()])
        query = f"DELETE FROM {table} WHERE {where_clause}"
        
        try:
            self.execute(query, tuple(where.values()), fetch=False)
            self.logger.info(f"Ma'lumot o'chirildi: {table}")
            return True
        except Exception as e:
            self.logger.error(f"Delete xatosi: {e}")
            return False
    
    def close(self):
        """Barcha ulanishlarni yopish"""
        if self.pool:
            self.pool.closeall()
            self.logger.info("Barcha ulanishlar yopildi")

# ----------------------------------------------------------------------------
# EXAMPLE USAGE
# ----------------------------------------------------------------------------

def main():
    """Local client misol"""
    
    # 1. Database URL orqali ulanish
    DATABASE_URL = "postgresql://postgres:password@localhost:5432/myapp_db"
    
    # 2. Environment variables orqali ulanish
    # export PGHOST=localhost
    # export PGPORT=5432
    # export PGDATABASE=myapp_db
    # export PGUSER=postgres
    # export PGPASSWORD=password
    
    # Klient yaratish
    db = PostgreSQLClient(DATABASE_URL)
    
    try:
        # 1. Test ulanish
        result = db.execute("SELECT version()")
        print(f"✅ PostgreSQL versiyasi: {result[0]['version']}")
        
        # 2. Table yaratish
        db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """, fetch=False)
        print("✅ Users table yaratildi")
        
        # 3. Ma'lumot qo'shish
        user_data = {
            'username': 'john_doe',
            'email': 'john@example.com'
        }
        db.insert('users', user_data)
        
        # 4. Ma'lumot o'qish
        users = db.select('users', limit=10)
        print(f"✅ Foydalanuvchilar: {len(users)} ta")
        for user in users:
            print(f"   - {user['username']}: {user['email']}")
        
        # 5. Ma'lumot yangilash
        db.update('users', 
                 {'email': 'john.doe@example.com'},
                 {'username': 'john_doe'})
        
        # 6. Statistika
        stats = db.execute("""
            SELECT 
                count(*) as total_users,
                count(DISTINCT username) as unique_users,
                max(created_at) as last_created
            FROM users
        """)
        print(f"📊 Statistika: {stats[0]}")
        
    except Exception as e:
        print(f"❌ Xato: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    import time
    main()
'''

# ============================================================================
# PYTHON CLIENT EXAMPLES - REMOTE/CLOUD VERSION
# ============================================================================

PYTHON_REMOTE_CLIENT = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PostgreSQL REMOTE/CLOUD Client Example
Muallif: DevOps Team
Version: 2.0.0
Environment: Production/Cloud
Features: SSL, Connection Pool, Retry Logic, Monitoring
"""

import os
import json
import time
import ssl
import psycopg2
import psycopg2.pool
import psycopg2.extras
from psycopg2 import pool, errors
from typing import Dict, List, Optional, Any, Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import threading
import urllib.parse
from functools import wraps

# ----------------------------------------------------------------------------
# PRODUCTION CONFIGURATION
# ----------------------------------------------------------------------------

@dataclass
class ProductionConfig:
    """Production sozlamalari"""
    # Connection settings
    POOL_MIN_SIZE: int = 5
    POOL_MAX_SIZE: int = 20
    CONNECTION_TIMEOUT: int = 30
    STATEMENT_TIMEOUT: int = 30000  # ms
    IDLE_IN_TRANSACTION_TIMEOUT: int = 60000  # ms
    
    # Retry settings
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 1
    RETRY_BACKOFF: float = 2.0
    
    # SSL settings
    SSL_MODE: str = 'verify-full'
    SSL_CERT_PATH: str = '/etc/ssl/certs'
    
    # Monitoring
    ENABLE_METRICS: bool = True
    SLOW_QUERY_THRESHOLD: float = 1.0  # seconds
    
    # Backup
    AUTO_BACKUP: bool = True
    BACKUP_INTERVAL: int = 86400  # 24 hours

# ----------------------------------------------------------------------------
# DATABASE URL PARSER
# ----------------------------------------------------------------------------

class DatabaseURL:
    """Remote database URL parser with SSL support"""
    
    def __init__(self, url: str):
        parsed = urllib.parse.urlparse(url)
        
        self.scheme = parsed.scheme
        self.username = urllib.parse.unquote(parsed.username) if parsed.username else None
        self.password = urllib.parse.unquote(parsed.password) if parsed.password else None
        self.host = parsed.hostname or 'localhost'
        self.port = parsed.port or 5432
        self.database = parsed.path.lstrip('/') if parsed.path else None
        
        # Query parameters
        self.params = dict(urllib.parse.parse_qsl(parsed.query))
        
        # SSL parameters
        self.sslmode = self.params.get('sslmode', 'verify-full')
        self.sslrootcert = self.params.get('sslrootcert')
        self.sslcert = self.params.get('sslcert')
        self.sslkey = self.params.get('sslkey')
    
    def get_connection_params(self) -> Dict[str, Any]:
        """SSL va production sozlamalari bilan connection params"""
        params = {
            'host': self.host,
            'port': self.port,
            'user': self.username,
            'password': self.password,
            'dbname': self.database,
            'sslmode': self.sslmode,
            'connect_timeout': ProductionConfig.CONNECTION_TIMEOUT,
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5,
            'options': f'-c statement_timeout={ProductionConfig.STATEMENT_TIMEOUT} -c idle_in_transaction_session_timeout={ProductionConfig.IDLE_IN_TRANSACTION_TIMEOUT}'
        }
        
        # SSL sertifikatlar
        if self.sslrootcert:
            params['sslrootcert'] = self.sslrootcert
        if self.sslcert:
            params['sslcert'] = self.sslcert
        if self.sslkey:
            params['sslkey'] = self.sslkey
        
        return params

# ----------------------------------------------------------------------------
# DECORATORS
# ----------------------------------------------------------------------------

def retry_on_failure(max_retries: int = ProductionConfig.MAX_RETRIES):
    """Automatic retry decorator"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (errors.OperationalError, errors.InterfaceError) as e:
                    last_error = e
                    delay = ProductionConfig.RETRY_DELAY * (ProductionConfig.RETRY_BACKOFF ** attempt)
                    time.sleep(delay)
                    continue
            raise last_error
        return wrapper
    return decorator

def monitor_query(func):
    """Query monitoring decorator"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        start_time = time.time()
        result = func(self, *args, **kwargs)
        duration = time.time() - start_time
        
        if duration > ProductionConfig.SLOW_QUERY_THRESHOLD:
            self.logger.warning(f"SLOW QUERY: {duration:.2f}s - {args[0][:100]}...")
        
        if hasattr(self, 'metrics'):
            self.metrics['queries'] += 1
            self.metrics['total_time'] += duration
            if duration > self.metrics['slowest_query_time']:
                self.metrics['slowest_query_time'] = duration
                self.metrics['slowest_query'] = args[0][:200]
        
        return result
    return wrapper

# ----------------------------------------------------------------------------
# PRODUCTION DATABASE CLIENT
# ----------------------------------------------------------------------------

class ProductionPostgreSQLClient:
    """
    PostgreSQL REMOTE/CLOUD Production Client
    - SSL/TLS encryption
    - Connection pooling
    - Automatic retry
    - Query monitoring
    - Metrics collection
    - Health checks
    """
    
    def __init__(self, database_url: str, app_name: str = None):
        """
        Production client
        
        Args:
            database_url: PostgreSQL URL (postgresql://user:pass@host:port/db?sslmode=require)
            app_name: Application nomi
        """
        self.db_url = DatabaseURL(database_url)
        self.app_name = app_name or 'ProductionApp'
        self.pool = None
        self.metrics = {
            'queries': 0,
            'total_time': 0,
            'slowest_query_time': 0,
            'slowest_query': '',
            'connections_created': 0,
            'connections_closed': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
        
        self.setup_logging()
        self.create_pool()
        self.setup_monitoring()
    
    def setup_logging(self):
        """Production logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'/var/log/postgresql/{self.app_name}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(f'PostgreSQL-Production-{self.app_name}')
    
    def create_pool(self):
        """Production connection pool"""
        try:
            conn_params = self.db_url.get_connection_params()
            conn_params['application_name'] = self.app_name
            
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                ProductionConfig.POOL_MIN_SIZE,
                ProductionConfig.POOL_MAX_SIZE,
                **conn_params
            )
            
            self.metrics['connections_created'] = ProductionConfig.POOL_MIN_SIZE
            self.logger.info(f"Production pool yaratildi: {ProductionConfig.POOL_MIN_SIZE}-{ProductionConfig.POOL_MAX_SIZE}")
            self.logger.info(f"SSL Mode: {self.db_url.sslmode}")
            
        except Exception as e:
            self.logger.error(f"Pool yaratishda xato: {e}")
            raise
    
    @contextmanager
    @retry_on_failure()
    def get_cursor(self, cursor_factory=psycopg2.extras.RealDictCursor):
        """Cursor olish with retry"""
        conn = None
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor(cursor_factory=cursor_factory)
            yield cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            self.metrics['errors'] += 1
            self.logger.error(f"Cursor xatosi: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.putconn(conn)
    
    @monitor_query
    @retry_on_failure()
    def execute(self, query: str, params: tuple = None, fetch: bool = True):
        """Query bajarish with monitoring"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            if fetch and cursor.description:
                return cursor.fetchall()
            return None
    
    def execute_many(self, query: str, params_list: List[tuple]):
        """Ko'p querylarni bajarish"""
        with self.get_cursor() as cursor:
            cursor.executemany(query, params_list)
    
    def transaction(self, queries: List[tuple]):
        """Transaction bajarish"""
        with self.get_cursor() as cursor:
            for query, params in queries:
                cursor.execute(query, params)
    
    def health_check(self) -> Dict[str, Any]:
        """Database health check"""
        health = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'app_name': self.app_name
        }
        
        try:
            # Test query
            start = time.time()
            result = self.execute("SELECT 1 as health_check", fetch=True)
            health['response_time'] = time.time() - start
            health['connection_status'] = 'ok'
            
            # Pool status
            health['pool'] = {
                'min': ProductionConfig.POOL_MIN_SIZE,
                'max': ProductionConfig.POOL_MAX_SIZE,
                'current_connections': self.pool._pool.qsize() if hasattr(self.pool, '_pool') else 'N/A'
            }
            
        except Exception as e:
            health['status'] = 'unhealthy'
            health['error'] = str(e)
        
        return health
    
    def get_metrics(self) -> Dict[str, Any]:
        """Performance metrics"""
        metrics = self.metrics.copy()
        metrics['uptime'] = str(datetime.now() - metrics['start_time'])
        metrics['avg_query_time'] = metrics['total_time'] / metrics['queries'] if metrics['queries'] > 0 else 0
        metrics['queries_per_second'] = metrics['queries'] / (datetime.now() - metrics['start_time']).total_seconds()
        
        return metrics
    
    def setup_monitoring(self):
        """Monitoring thread"""
        if ProductionConfig.ENABLE_METRICS:
            def monitor():
                while True:
                    time.sleep(60)  # Har daqiqada
                    try:
                        health = self.health_check()
                        if health['status'] != 'healthy':
                            self.logger.warning(f"Health check failed: {health}")
                        
                        # Slow log analysis
                        self.logger.info(f"Metrics: {self.get_metrics()['queries_per_second']:.2f} qps")
                        
                    except Exception as e:
                        self.logger.error(f"Monitoring xatosi: {e}")
            
            thread = threading.Thread(target=monitor, daemon=True)
            thread.start()
    
    def close(self):
        """Barcha ulanishlarni yopish"""
        if self.pool:
            self.pool.closeall()
            self.metrics['connections_closed'] = self.metrics['connections_created']
            self.logger.info("Production ulanishlar yopildi")

# ----------------------------------------------------------------------------
# EXAMPLE USAGE - PRODUCTION
# ----------------------------------------------------------------------------

def main_production():
    """Production misol"""
    
    # 1. Production Database URL
    DATABASE_URL = "postgresql://app_user:SecurePass123!@db.production.internal:5432/production_db?sslmode=verify-full&sslrootcert=/etc/ssl/certs/ca.pem"
    
    # 2. Client yaratish
    db = ProductionPostgreSQLClient(DATABASE_URL, app_name="MyProductionApp")
    
    try:
        # 3. Health check
        health = db.health_check()
        print(f"🏥 Health Status: {health['status']}")
        print(f"⏱️  Response Time: {health['response_time']:.3f}s")
        
        # 4. Transaction
        db.transaction([
            ("INSERT INTO audit_log (action, user_id) VALUES (%s, %s)", ("login", 1)),
            ("UPDATE users SET last_login = NOW() WHERE id = %s", (1,))
        ])
        
        # 5. Complex query
        results = db.execute("""
            SELECT 
                u.username,
                COUNT(o.id) as order_count,
                SUM(o.total) as total_spent,
                AVG(o.total) as avg_order_value
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.created_at > %s
            GROUP BY u.id, u.username
            HAVING COUNT(o.id) > 0
            ORDER BY total_spent DESC
            LIMIT 10
        """, (datetime.now() - timedelta(days=30),))
        
        print(f"📊 Top 10 mijozlar:")
        for row in results:
            print(f"   {row['username']}: {row['order_count']} orders, ${row['total_spent']:.2f}")
        
        # 6. Performance metrics
        metrics = db.get_metrics()
        print(f"\n📈 Performance Metrics:")
        print(f"   Queries: {metrics['queries']}")
        print(f"   Avg Time: {metrics['avg_query_time']:.3f}s")
        print(f"   QPS: {metrics['queries_per_second']:.2f}")
        
    except Exception as e:
        print(f"❌ Xato: {e}")
    finally:
        db.close()

# ----------------------------------------------------------------------------
# ASYNCIO VERSION
# ----------------------------------------------------------------------------

import asyncio
import asyncpg
from asyncpg.pool import Pool

class AsyncPostgreSQLClient:
    """Asynchronous PostgreSQL client"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: Optional[Pool] = None
    
    async def create_pool(self):
        """Async connection pool"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=5,
            max_size=20,
            command_timeout=30,
            max_queries=50000,
            max_inactive_connection_lifetime=300
        )
        return self.pool
    
    async def execute(self, query: str, *args):
        """Async query execution"""
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def close(self):
        """Close pool"""
        if self.pool:
            await self.pool.close()

async def async_example():
    """Async misol"""
    db = AsyncPostgreSQLClient("postgresql://user:pass@localhost/db")
    await db.create_pool()
    
    try:
        users = await db.execute("SELECT * FROM users LIMIT 10")
        print(f"Users: {len(users)}")
    finally:
        await db.close()

if __name__ == "__main__":
    # Production versiya
    main_production()
    
    # Async versiya
    # asyncio.run(async_example())
'''

# ============================================================================
# DEPLOYMENT GENERATOR
# ============================================================================

class DeploymentGenerator:
    """Deployment fayllarini generatsiya qilish"""
    
    @staticmethod
    def generate_docker_compose(deployment: DeploymentConfig) -> str:
        """Docker Compose fayl generatsiya qilish"""
        return f'''version: '3.8'

services:
  postgres-{deployment.name}:
    image: postgres:14-alpine
    container_name: postgres-{deployment.name}
    environment:
      POSTGRES_DB: {deployment.database_url.database}
      POSTGRES_USER: {deployment.database_url.username}
      POSTGRES_PASSWORD: {deployment.database_url.password}
      POSTGRES_INITDB_ARGS: "--data-checksums"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./backups:/backups
      - ./init:/docker-entrypoint-initdb.d
    ports:
      - "{deployment.database_url.port}:5432"
    networks:
      - app_network
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U {deployment.database_url.username}"]
      interval: 10s
      timeout: 5s
      retries: 5
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  pgadmin-{deployment.name}:
    image: dpage/pgadmin4
    container_name: pgadmin-{deployment.name}
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@{deployment.name}.com
      PGADMIN_DEFAULT_PASSWORD: {deployment.database_url.password}
      PGADMIN_CONFIG_SERVER_MODE: 'False'
    volumes:
      - pgadmin_data:/var/lib/pgadmin
    ports:
      - "5050:80"
    networks:
      - app_network
    restart: unless-stopped
    depends_on:
      - postgres-{deployment.name}

volumes:
  postgres_data:
  pgadmin_data:

networks:
  app_network:
    driver: bridge
'''
    
    @staticmethod
    def generate_kubernetes(deployment: DeploymentConfig) -> str:
        """Kubernetes manifest generatsiya qilish"""
        return f'''# PostgreSQL StatefulSet
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres-{deployment.name}
  namespace: {deployment.environment.value}
spec:
  serviceName: postgres-{deployment.name}
  replicas: 1
  selector:
    matchLabels:
      app: postgres-{deployment.name}
  template:
    metadata:
      labels:
        app: postgres-{deployment.name}
    spec:
      containers:
      - name: postgres
        image: postgres:14-alpine
        ports:
        - containerPort: 5432
          name: postgres
        env:
        - name: POSTGRES_DB
          value: "{deployment.database_url.database}"
        - name: POSTGRES_USER
          value: "{deployment.database_url.username}"
        - name: POSTGRES_PASSWORD
          value: "{deployment.database_url.password}"
        - name: PGDATA
          value: /var/lib/postgresql/data/pgdata
        volumeMounts:
        - name: postgres-storage
          mountPath: /var/lib/postgresql/data
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          exec:
            command:
            - pg_isready
            - -U
            - {deployment.database_url.username}
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          exec:
            command:
            - pg_isready
            - -U
            - {deployment.database_url.username}
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: postgres-storage
        persistentVolumeClaim:
          claimName: postgres-pvc-{deployment.name}
---
# Persistent Volume Claim
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-pvc-{deployment.name}
  namespace: {deployment.environment.value}
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
---
# Service
apiVersion: v1
kind: Service
metadata:
  name: postgres-{deployment.name}
  namespace: {deployment.environment.value}
spec:
  selector:
    app: postgres-{deployment.name}
  ports:
  - port: 5432
    targetPort: 5432
  type: ClusterIP
---
# ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-config-{deployment.name}
  namespace: {deployment.environment.value}
data:
  postgresql.conf: |
    max_connections = 100
    shared_buffers = 256MB
    effective_cache_size = 768MB
    maintenance_work_mem = 64MB
    checkpoint_completion_target = 0.9
    wal_buffers = 16MB
    default_statistics_target = 100
    random_page_cost = 1.1
    effective_io_concurrency = 200
    work_mem = 4MB
    min_wal_size = 1GB
    max_wal_size = 4GB
    max_worker_processes = 4
    max_parallel_workers_per_gather = 2
    max_parallel_workers = 4
    max_parallel_maintenance_workers = 2
'''
    
    @staticmethod
    def generate_env_file(deployment: DeploymentConfig) -> str:
        """Environment fayl generatsiya qilish"""
        return f'''# PostgreSQL Environment Configuration
# Generated: {datetime.datetime.now()}
# Environment: {deployment.environment.value}
# Deployment: {deployment.deployment_type.value}

# Database Connection
PGHOST={deployment.database_url.host}
PGPORT={deployment.database_url.port}
PGDATABASE={deployment.database_url.database}
PGUSER={deployment.database_url.username}
PGPASSWORD={deployment.database_url.password}
DATABASE_URL={deployment.database_url.to_string()}

# SSL Configuration
PGSSLMODE={deployment.database_url.params.get('sslmode', 'require')}

# Connection Pool
POOL_MIN_SIZE=5
POOL_MAX_SIZE={deployment.pool_size}
POOL_TIMEOUT={deployment.timeout}

# Application
APP_ENV={deployment.environment.value}
APP_NAME={deployment.name}
DEBUG={'true' if deployment.environment == EnvironmentType.DEVELOPMENT else 'false'}

# Backup
BACKUP_ENABLED={str(deployment.backup_enabled).lower()}
BACKUP_PATH=/backups/{deployment.name}
BACKUP_RETENTION_DAYS=30

# Monitoring
MONITORING_ENABLED={str(deployment.monitoring_enabled).lower()}
METRICS_PORT=9187
'''

# ============================================================================
# INTERFEYS (UI)
# ============================================================================

class PostgreSQLEnterpriseUI:
    """Asosiy interfeys"""
    
    def __init__(self):
        self.url_manager = DatabaseURLManager()
        self.deployment_manager = DeploymentManager()
        self.current_db_url = None
        self.current_deployment = None
        self.running = True
    
    def clear_screen(self):
        """Ekran tozalash"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def print_header(self):
        """Header chiqarish"""
        self.clear_screen()
        print(Fore.CYAN + "=" * 90)
        print(Fore.GREEN + """
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║                                                                          ║
    ║     ██████╗  ██████╗ ███████╗████████╗ ██████╗ ██████╗ ███████╗███████╗ ║
    ║     ██╔══██╗██╔═══██╗██╔════╝╚══██╔══╝██╔════╝ ██╔══██╗██╔════╝██╔════╝ ║
    ║     ██████╔╝██║   ██║███████╗   ██║   ██║  ███╗██████╔╝█████╗  █████╗   ║
    ║     ██╔═══╝ ██║   ██║╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  ██╔══╝   ║
    ║     ██║     ╚██████╔╝███████║   ██║   ╚██████╔╝██║  ██║███████╗███████╗ ║
    ║     ╚═╝      ╚═════╝ ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝ ║
    ║                                                                          ║
    ║              🔗 DATABASE URL & DEPLOYMENT MANAGEMENT SYSTEM              ║
    ║                         LOCAL + REMOTE + CLOUD                          ║
    ║                              Version 4.0.0                              ║
    ║                                                                          ║
    ╚══════════════════════════════════════════════════════════════════════════╝
        """ + Style.RESET_ALL)
        print(Fore.CYAN + "=" * 90 + Style.RESET_ALL)
        
        # Status panel
        print(Fore.YELLOW + "\n📊 SYSTEM STATUS:" + Style.RESET_ALL)
        print(f"   • Database URLs: {Fore.GREEN}{len(self.url_manager.urls)}{Style.RESET_ALL}")
        print(f"   • Deployments: {Fore.GREEN}{len(self.deployment_manager.deployments)}{Style.RESET_ALL}")
        print(f"   • Active URL: {Fore.CYAN}{self.current_db_url.to_string(hide_password=True) if self.current_db_url else 'None'}{Style.RESET_ALL}")
        print(Fore.CYAN + "-" * 90 + Style.RESET_ALL)
    
    def print_menu(self):
        """Asosiy menyu"""
        print(Fore.CYAN + """
╔══════════════════════════════════════════════════════════════════════════╗
║                             📋 ASOSIY MENYU                               ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  🔗 DATABASE URL BOSHQARISH              🚀 DEPLOYMENT BOSHQARISH        ║
║  ──────────────────────────              ──────────────────────────      ║
║   1. Yangi Database URL qo'shish         11. Yangi deployment yaratish  ║
║   2. Database URL larni ko'rish          12. Deployment larni ko'rish   ║
║   3. Database URL tanlash                13. Deployment tanlash         ║
║   4. URL ni tahrirlash                  14. Deployment o'chirish        ║
║   5. URL ni o'chirish                   15. Deployment test qilish      ║
║   6. URL ni test qilish                 16. Deployment loglari          ║
║   7. URL generatsiya qilish             17. Deployment config export    ║
║                                                                          ║
║  💾 DATABASE MA'LUMOTLARI                 📊 MONITORING                  ║
║  ────────────────────────                 ────────────────────────       ║
║   8. Ma'lumot qo'shish (Single)          18. Deployment monitoring      ║
║   9. Ma'lumot qo'shish (Bulk)           19. Health check               ║
║  10. CSV/JSON import                    20. Performance metrics         ║
║                                                                          ║
║  🐍 PYTHON MISOL GENERATOR               ⚙️  SOZLAMALAR                 ║
║  ────────────────────────                 ────────────────────────       ║
║  21. Local client generator             23. Docker Compose generate     ║
║  22. Remote/Cloud client generator      24. Kubernetes generate         ║
║                                         25. Environment file generate   ║
║                                                                          ║
║  0. Chiqish                                                             ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""" + Style.RESET_ALL)
    
    def run(self):
        """Asosiy sikl"""
        while self.running:
            self.print_header()
            self.print_menu()
            
            choice = input(Fore.YELLOW + "🔷 Tanlang [0-25]: " + Style.RESET_ALL)
            
            if choice == '1':
                self._add_database_url_ui()
            elif choice == '2':
                self._list_database_urls_ui()
            elif choice == '3':
                self._select_database_url_ui()
            elif choice == '4':
                self._edit_database_url_ui()
            elif choice == '5':
                self._remove_database_url_ui()
            elif choice == '6':
                self._test_database_url_ui()
            elif choice == '7':
                self._generate_database_url_ui()
            elif choice == '8':
                self._insert_single_ui()
            elif choice == '9':
                self._insert_bulk_ui()
            elif choice == '10':
                self._import_file_ui()
            elif choice == '11':
                self._create_deployment_ui()
            elif choice == '12':
                self._list_deployments_ui()
            elif choice == '13':
                self._select_deployment_ui()
            elif choice == '14':
                self._remove_deployment_ui()
            elif choice == '15':
                self._test_deployment_ui()
            elif choice == '16':
                self._deployment_logs_ui()
            elif choice == '17':
                self._export_deployment_config_ui()
            elif choice == '18':
                self._deployment_monitoring_ui()
            elif choice == '19':
                self._health_check_ui()
            elif choice == '20':
                self._performance_metrics_ui()
            elif choice == '21':
                self._generate_local_client_ui()
            elif choice == '22':
                self._generate_remote_client_ui()
            elif choice == '23':
                self._generate_docker_compose_ui()
            elif choice == '24':
                self._generate_kubernetes_ui()
            elif choice == '25':
                self._generate_env_file_ui()
            elif choice == '0':
                self._exit_ui()
            else:
                print(Fore.RED + "❌ Noto'g'ri tanlov!" + Style.RESET_ALL)
                time.sleep(1)
    
    # ------------------------------------------------------------------------
    # DATABASE URL UI
    # ------------------------------------------------------------------------
    
    def _add_database_url_ui(self):
        """Yangi Database URL qo'shish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔗 YANGI DATABASE URL QO'SHISH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        print("\n📌 Database URL formatlari:")
        print("   1. Full URL: postgresql://user:pass@host:5432/db?sslmode=require")
        print("   2. Manual kiritish")
        
        choice = input("\nTanlang [1-2]: ").strip()
        
        if choice == '1':
            url_str = input("Database URL: ").strip()
            try:
                url = DatabaseURL.from_string(url_str)
                name = input("URL nomi: ").strip()
                
                if self.url_manager.add_url(name, url):
                    print(Fore.GREEN + f"\n✅ URL '{name}' qo'shildi!" + Style.RESET_ALL)
            except Exception as e:
                print(Fore.RED + f"\n❌ Xato: {e}" + Style.RESET_ALL)
        
        elif choice == '2':
            print("\n📝 Ma'lumotlarni kiriting:")
            name = input("  URL nomi: ").strip()
            host = input("  Host [localhost]: ").strip() or "localhost"
            port = input("  Port [5432]: ").strip() or "5432"
            database = input("  Database nomi: ").strip()
            username = input("  Username [postgres]: ").strip() or "postgres"
            password = getpass.getpass("  Password: ").strip()
            
            sslmode = input("  SSL mode [prefer]: ").strip() or "prefer"
            
            url = DatabaseURL(
                username=username,
                password=password,
                host=host,
                port=int(port),
                database=database,
                params={'sslmode': sslmode}
            )
            
            if self.url_manager.add_url(name, url):
                print(Fore.GREEN + f"\n✅ URL '{name}' qo'shildi!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _list_database_urls_ui(self):
        """Database URL larni ko'rish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔗 DATABASE URL LAR RO'YXATI" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 80 + Style.RESET_ALL)
        
        if not self.url_manager.urls:
            print("Hech qanday URL mavjud emas")
        else:
            table = PrettyTable()
            table.field_names = ["No", "Nomi", "Database URL", "Status"]
            table.align["Nomi"] = "l"
            table.align["Database URL"] = "l"
            
            for idx, (name, url) in enumerate(self.url_manager.urls.items(), 1):
                url_str = url.to_string(hide_password=True)
                status = Fore.GREEN + "✓" + Style.RESET_ALL if url == self.current_db_url else ""
                table.add_row([idx, name, url_str, status])
            
            print(table)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _select_database_url_ui(self):
        """Database URL tanlash"""
        self._list_database_urls_ui()
        
        if self.url_manager.urls:
            name = input("\nTanlash uchun URL nomi: ").strip()
            url = self.url_manager.get_url(name)
            if url:
                self.current_db_url = url
                print(Fore.GREEN + f"\n✅ URL '{name}' tanlandi!" + Style.RESET_ALL)
            else:
                print(Fore.RED + f"\n❌ URL '{name}' topilmadi!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _test_database_url_ui(self):
        """Database URL test qilish"""
        if not self.current_db_url:
            print(Fore.RED + "\n❌ Avval URL tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        print(Fore.YELLOW + f"\n🔍 Test qilinmoqda: {self.current_db_url.to_string(hide_password=True)}" + Style.RESET_ALL)
        
        success, message = self.url_manager.test_connection(self.current_db_url)
        
        if success:
            print(Fore.GREEN + f"\n✅ {message}" + Style.RESET_ALL)
            
            # Qo'shimcha ma'lumot
            try:
                inserter = DataInsertionManager(self.current_db_url)
                result = inserter.execute_query("SELECT version()")
                if result:
                    print(f"\n📌 PostgreSQL versiyasi: {result[0]['version']}")
            except:
                pass
        else:
            print(Fore.RED + f"\n❌ {message}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _generate_database_url_ui(self):
        """Yangi URL generatsiya qilish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🔨 DATABASE URL GENERATSIYA" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        print("\nTanlang:")
        print("  1. Local development")
        print("  2. Remote production")
        print("  3. Cloud (AWS RDS, Google Cloud SQL)")
        print("  4. Docker container")
        
        choice = input("\nTanlang [1-4]: ").strip()
        
        if choice == '1':
            url = DatabaseURL(
                host='localhost',
                port=5432,
                database='myapp_dev',
                username='postgres',
                password='postgres'
            )
            print(Fore.GREEN + f"\n✅ Local URL: {url.to_string()}" + Style.RESET_ALL)
            
        elif choice == '2':
            url = DatabaseURL(
                host='db.production.internal',
                port=5432,
                database='production_db',
                username='app_user',
                password='CHANGE_ME_NOW',
                params={'sslmode': 'require', 'connect_timeout': '30'}
            )
            print(Fore.GREEN + f"\n✅ Remote URL: {url.to_string()}" + Style.RESET_ALL)
            
        elif choice == '3':
            print(Fore.YELLOW + "\n☁️ Cloud URL formatlari:" + Style.RESET_ALL)
            print("  AWS RDS: postgresql://username:password@hostname:5432/dbname?sslmode=require")
            print("  GCP SQL: postgresql://username:password@/dbname?host=/cloudsql/project:region:instance")
            print("  Azure DB: postgresql://username@servername:password@servername.postgres.database.azure.com:5432/dbname")
            
        elif choice == '4':
            url = DatabaseURL(
                host='postgres_container',
                port=5432,
                database='app_db',
                username='app_user',
                password='docker_pass123'
            )
            print(Fore.GREEN + f"\n✅ Docker URL: {url.to_string()}" + Style.RESET_ALL)
        
        save = input("\nBu URL ni saqlaymizmi? [y/N]: ").strip().lower()
        if save == 'y':
            name = input("URL nomi: ").strip()
            self.url_manager.add_url(name, url)
        
        input("\nDavom etish uchun Enter bosing...")
    
    # ------------------------------------------------------------------------
    # DATA INSERTION UI
    # ------------------------------------------------------------------------
    
    def _insert_single_ui(self):
        """Bitta ma'lumot qo'shish"""
        if not self.current_db_url:
            print(Fore.RED + "\n❌ Avval URL tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        self.clear_screen()
        print(Fore.CYAN + "\n📝 MA'LUMOT QO'SHISH (SINGLE)" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        table = input("Table nomi: ").strip()
        
        print("\nMa'lumotlarni JSON formatda kiriting:")
        print("  Masalan: {\"name\": \"John\", \"email\": \"john@example.com\"}")
        
        try:
            data_str = input("Data: ").strip()
            data = json.loads(data_str)
            
            inserter = DataInsertionManager(self.current_db_url)
            if inserter.insert_single(table, data):
                print(Fore.GREEN + f"\n✅ Ma'lumot qo'shildi!" + Style.RESET_ALL)
            else:
                print(Fore.RED + f"\n❌ Xato yuz berdi!" + Style.RESET_ALL)
                
        except json.JSONDecodeError:
            print(Fore.RED + "\n❌ Noto'g'ri JSON format!" + Style.RESET_ALL)
        except Exception as e:
            print(Fore.RED + f"\n❌ Xato: {e}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _insert_bulk_ui(self):
        """Ko'p ma'lumot qo'shish"""
        if not self.current_db_url:
            print(Fore.RED + "\n❌ Avval URL tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        self.clear_screen()
        print(Fore.CYAN + "\n📦 MA'LUMOT QO'SHISH (BULK)" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        table = input("Table nomi: ").strip()
        
        print("\nMa'lumotlarni JSON array formatida kiriting:")
        print("  Masalan: [{\"name\": \"John\", \"age\": 30}, {\"name\": \"Jane\", \"age\": 25}]")
        
        try:
            data_str = input("Data: ").strip()
            data = json.loads(data_str)
            
            if not isinstance(data, list):
                data = [data]
            
            inserter = DataInsertionManager(self.current_db_url)
            successful, failed = inserter.insert_bulk(table, data)
            
            print(Fore.GREEN + f"\n✅ Muvaffaqiyatli: {successful}" + Style.RESET_ALL)
            if failed > 0:
                print(Fore.RED + f"❌ Xatolik: {failed}" + Style.RESET_ALL)
                
        except json.JSONDecodeError:
            print(Fore.RED + "\n❌ Noto'g'ri JSON format!" + Style.RESET_ALL)
        except Exception as e:
            print(Fore.RED + f"\n❌ Xato: {e}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _import_file_ui(self):
        """CSV/JSON fayldan import"""
        if not self.current_db_url:
            print(Fore.RED + "\n❌ Avval URL tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        self.clear_screen()
        print(Fore.CYAN + "\n📁 CSV/JSON IMPORT" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        file_path = input("Fayl yo'li: ").strip()
        table = input("Table nomi: ").strip()
        
        if not os.path.exists(file_path):
            print(Fore.RED + "\n❌ Fayl topilmadi!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        inserter = DataInsertionManager(self.current_db_url)
        
        if file_path.endswith('.csv'):
            delimiter = input("CSV delimiter [default: ,]: ").strip() or ','
            successful, failed = inserter.insert_from_csv(table, file_path, delimiter)
        elif file_path.endswith('.json'):
            successful, failed = inserter.insert_from_json(table, file_path)
        else:
            print(Fore.RED + "\n❌ Faqat CSV yoki JSON fayllar qo'llab-quvvatlanadi!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        print(Fore.GREEN + f"\n✅ Import: {successful} ta qo'shildi" + Style.RESET_ALL)
        if failed > 0:
            print(Fore.RED + f"❌ Xatolik: {failed} ta" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    # ------------------------------------------------------------------------
    # DEPLOYMENT UI
    # ------------------------------------------------------------------------
    
    def _create_deployment_ui(self):
        """Yangi deployment yaratish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🚀 YANGI DEPLOYMENT YARATISH" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        # URL tanlash
        if not self.url_manager.urls:
            print(Fore.RED + "❌ Avval Database URL qo'shing!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        print("\n📌 Mavjud URL lar:")
        for name in self.url_manager.urls.keys():
            print(f"   • {name}")
        
        url_name = input("\nURL nomi: ").strip()
        database_url = self.url_manager.get_url(url_name)
        
        if not database_url:
            print(Fore.RED + "❌ URL topilmadi!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        # Deployment ma'lumotlari
        name = input("\nDeployment nomi: ").strip()
        
        print("\n🌍 Muhit (Environment):")
        for env in EnvironmentType:
            print(f"   {env.value}) {env.name}")
        env_choice = input("Tanlang: ").strip()
        env_map = {e.value: e for e in EnvironmentType}
        environment = env_map.get(env_choice, EnvironmentType.DEVELOPMENT)
        
        print("\n📦 Deployment turi:")
        for dep in DeploymentType:
            print(f"   {dep.value}) {dep.name}")
        dep_choice = input("Tanlang: ").strip()
        dep_map = {d.value: d for d in DeploymentType}
        deployment_type = dep_map.get(dep_choice, DeploymentType.LOCAL)
        
        print("\n🔌 Ulanish rejimi:")
        for mode in ConnectionMode:
            print(f"   {mode.value}) {mode.name}")
        mode_choice = input("Tanlang: ").strip()
        mode_map = {m.value: m for m in ConnectionMode}
        connection_mode = mode_map.get(mode_choice, ConnectionMode.DIRECT)
        
        # Qo'shimcha sozlamalar
        pool_size = input("Pool size [20]: ").strip() or 20
        timeout = input("Timeout [30]: ").strip() or 30
        
        if self.deployment_manager.create_deployment(
            name=name,
            environment=environment,
            deployment_type=deployment_type,
            connection_mode=connection_mode,
            database_url=database_url,
            pool_size=int(pool_size),
            timeout=int(timeout)
        ):
            print(Fore.GREEN + f"\n✅ Deployment '{name}' yaratildi!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _list_deployments_ui(self):
        """Deployment larni ko'rish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🚀 DEPLOYMENT LAR RO'YXATI" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 90 + Style.RESET_ALL)
        
        if not self.deployment_manager.deployments:
            print("Hech qanday deployment mavjud emas")
        else:
            table = PrettyTable()
            table.field_names = ["No", "Nomi", "Muhit", "Turi", "Ulanish", "Database", "Status"]
            table.align = "l"
            
            for idx, (name, dep) in enumerate(self.deployment_manager.deployments.items(), 1):
                status = Fore.GREEN + "✓" + Style.RESET_ALL if name == self.current_deployment else ""
                table.add_row([
                    idx,
                    name,
                    dep.environment.value,
                    dep.deployment_type.value,
                    dep.connection_mode.value,
                    dep.database_url.database or '-',
                    status
                ])
            
            print(table)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _select_deployment_ui(self):
        """Deployment tanlash"""
        self._list_deployments_ui()
        
        if self.deployment_manager.deployments:
            name = input("\nTanlash uchun deployment nomi: ").strip()
            deployment = self.deployment_manager.get_deployment(name)
            if deployment:
                self.current_deployment = name
                self.current_db_url = deployment.database_url
                print(Fore.GREEN + f"\n✅ Deployment '{name}' tanlandi!" + Style.RESET_ALL)
            else:
                print(Fore.RED + f"\n❌ Deployment '{name}' topilmadi!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _test_deployment_ui(self):
        """Deployment test qilish"""
        if not self.current_deployment:
            print(Fore.RED + "\n❌ Avval deployment tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        deployment = self.deployment_manager.get_deployment(self.current_deployment)
        
        print(Fore.YELLOW + f"\n🔍 Test qilinmoqda: {deployment.name}" + Style.RESET_ALL)
        
        # Connection test
        success, message = self.url_manager.test_connection(deployment.database_url)
        
        if success:
            print(Fore.GREEN + f"   ✅ Database: {message}" + Style.RESET_ALL)
            
            # SSL test
            if deployment.ssl_enabled:
                print(Fore.GREEN + f"   ✅ SSL: Enabled" + Style.RESET_ALL)
            
            # Pool test
            print(Fore.GREEN + f"   ✅ Pool: {deployment.pool_size} connections" + Style.RESET_ALL)
            
        else:
            print(Fore.RED + f"   ❌ Database: {message}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    # ------------------------------------------------------------------------
    # GENERATOR UI
    # ------------------------------------------------------------------------
    
    def _generate_local_client_ui(self):
        """Local Python client generatsiya qilish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🐍 LOCAL PYTHON CLIENT GENERATSIYA" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        filename = input("Fayl nomi [postgres_local_client.py]: ").strip() or "postgres_local_client.py"
        
        with open(filename, 'w') as f:
            f.write(PYTHON_LOCAL_CLIENT)
        
        os.chmod(filename, 0o755)
        print(Fore.GREEN + f"\n✅ Local client generatsiya qilindi: {filename}" + Style.RESET_ALL)
        print(f"\n📌 Ishlatish:")
        print(f"   python3 {filename}")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _generate_remote_client_ui(self):
        """Remote Python client generatsiya qilish"""
        self.clear_screen()
        print(Fore.CYAN + "\n🐍 REMOTE/CLOUD PYTHON CLIENT GENERATSIYA" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        filename = input("Fayl nomi [postgres_remote_client.py]: ").strip() or "postgres_remote_client.py"
        
        with open(filename, 'w') as f:
            f.write(PYTHON_REMOTE_CLIENT)
        
        os.chmod(filename, 0o755)
        print(Fore.GREEN + f"\n✅ Remote client generatsiya qilindi: {filename}" + Style.RESET_ALL)
        print(f"\n📌 Ishlatish:")
        print(f"   python3 {filename}")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _generate_docker_compose_ui(self):
        """Docker Compose fayl generatsiya qilish"""
        if not self.current_deployment:
            print(Fore.RED + "\n❌ Avval deployment tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        deployment = self.deployment_manager.get_deployment(self.current_deployment)
        
        filename = input("Fayl nomi [docker-compose.yml]: ").strip() or "docker-compose.yml"
        
        content = DeploymentGenerator.generate_docker_compose(deployment)
        
        with open(filename, 'w') as f:
            f.write(content)
        
        print(Fore.GREEN + f"\n✅ Docker Compose generatsiya qilindi: {filename}" + Style.RESET_ALL)
        print(f"\n📌 Ishlatish:")
        print(f"   docker-compose up -d")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _generate_kubernetes_ui(self):
        """Kubernetes manifest generatsiya qilish"""
        if not self.current_deployment:
            print(Fore.RED + "\n❌ Avval deployment tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        deployment = self.deployment_manager.get_deployment(self.current_deployment)
        
        filename = input("Fayl nomi [postgres-k8s.yaml]: ").strip() or "postgres-k8s.yaml"
        
        content = DeploymentGenerator.generate_kubernetes(deployment)
        
        with open(filename, 'w') as f:
            f.write(content)
        
        print(Fore.GREEN + f"\n✅ Kubernetes manifest generatsiya qilindi: {filename}" + Style.RESET_ALL)
        print(f"\n📌 Ishlatish:")
        print(f"   kubectl apply -f {filename}")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _generate_env_file_ui(self):
        """Environment fayl generatsiya qilish"""
        if not self.current_deployment:
            print(Fore.RED + "\n❌ Avval deployment tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        deployment = self.deployment_manager.get_deployment(self.current_deployment)
        
        filename = input("Fayl nomi [.env]: ").strip() or ".env"
        
        content = DeploymentGenerator.generate_env_file(deployment)
        
        with open(filename, 'w') as f:
            f.write(content)
        
        os.chmod(filename, 0o600)  # Faqat owner o'qiy olsin
        print(Fore.GREEN + f"\n✅ Environment fayl generatsiya qilindi: {filename}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    # ------------------------------------------------------------------------
    # MONITORING UI
    # ------------------------------------------------------------------------
    
    def _deployment_monitoring_ui(self):
        """Deployment monitoring"""
        if not self.current_deployment:
            print(Fore.RED + "\n❌ Avval deployment tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        deployment = self.deployment_manager.get_deployment(self.current_deployment)
        
        self.clear_screen()
        print(Fore.CYAN + f"\n📊 DEPLOYMENT MONITORING: {deployment.name}" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        try:
            inserter = DataInsertionManager(deployment.database_url)
            
            # Aktiv ulanishlar
            connections = inserter.execute_query("""
                SELECT 
                    state,
                    count(*) as count,
                    count(DISTINCT usename) as users,
                    count(DISTINCT datname) as databases
                FROM pg_stat_activity 
                GROUP BY state
            """)
            
            print(Fore.YELLOW + "\n🔌 Ulanishlar:" + Style.RESET_ALL)
            for conn in connections or []:
                print(f"   {conn['state'] or 'unknown'}: {conn['count']} ta")
            
            # Database o'lchamlari
            db_sizes = inserter.execute_query("""
                SELECT 
                    datname,
                    pg_size_pretty(pg_database_size(datname)) as size
                FROM pg_database
                WHERE datistemplate = false
                ORDER BY pg_database_size(datname) DESC
                LIMIT 5
            """)
            
            print(Fore.YELLOW + "\n💾 Database o'lchamlari:" + Style.RESET_ALL)
            for db in db_sizes or []:
                print(f"   {db['datname']}: {db['size']}")
            
            # Cache hit ratio
            cache = inserter.execute_query("""
                SELECT 
                    sum(heap_blks_hit)::float / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100 as hit_ratio
                FROM pg_statio_user_tables
            """)
            
            if cache and cache[0]['hit_ratio']:
                print(Fore.YELLOW + "\n⚡ Cache hit ratio:" + Style.RESET_ALL)
                print(f"   {cache[0]['hit_ratio']:.2f}%")
            
        except Exception as e:
            print(Fore.RED + f"\n❌ Monitoring xatosi: {e}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _health_check_ui(self):
        """Health check"""
        if not self.current_db_url:
            print(Fore.RED + "\n❌ Avval URL tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        self.clear_screen()
        print(Fore.CYAN + "\n🏥 HEALTH CHECK" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        try:
            inserter = DataInsertionManager(self.current_db_url)
            
            start_time = time.time()
            result = inserter.execute_query("SELECT 1 as health_check")
            response_time = time.time() - start_time
            
            if result:
                print(Fore.GREEN + f"✅ Status: Healthy" + Style.RESET_ALL)
                print(f"   Response time: {response_time:.3f}s")
                
                # Server ma'lumotlari
                version = inserter.execute_query("SELECT version()")
                if version:
                    print(f"   Version: {version[0]['version'].split(',')[0]}")
                
                # Uptime
                uptime = inserter.execute_query("""
                    SELECT 
                        pg_postmaster_start_time() as start_time,
                        now() - pg_postmaster_start_time() as uptime
                """)
                if uptime:
                    print(f"   Uptime: {uptime[0]['uptime']}")
                
        except Exception as e:
            print(Fore.RED + f"❌ Status: Unhealthy" + Style.RESET_ALL)
            print(f"   Error: {e}")
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _performance_metrics_ui(self):
        """Performance metrics"""
        if not self.current_db_url:
            print(Fore.RED + "\n❌ Avval URL tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        self.clear_screen()
        print(Fore.CYAN + "\n📈 PERFORMANCE METRICS" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        try:
            inserter = DataInsertionManager(self.current_db_url)
            
            # Sekin querylar
            slow_queries = inserter.execute_query("""
                SELECT 
                    pid,
                    usename,
                    datname,
                    query,
                    age(now(), query_start) as duration
                FROM pg_stat_activity
                WHERE state = 'active' 
                  AND query NOT LIKE '%pg_stat_activity%'
                  AND age(now(), query_start) > interval '1 second'
                ORDER BY duration DESC
                LIMIT 5
            """)
            
            print(Fore.YELLOW + "\n🐌 Sekin querylar (1+ sekund):" + Style.RESET_ALL)
            if slow_queries:
                for q in slow_queries:
                    print(f"   • {q['duration']}: {q['query'][:50]}...")
            else:
                print("   Topilmadi")
            
            # Statistika
            stats = inserter.execute_query("""
                SELECT 
                    (SELECT count(*) FROM pg_stat_activity) as total_connections,
                    (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
                    (SELECT pg_size_pretty(sum(pg_database_size(datname))) FROM pg_database WHERE datistemplate = false) as total_db_size,
                    (SELECT count(*) FROM pg_stat_user_tables) as total_tables
            """)
            
            if stats:
                print(Fore.YELLOW + "\n📊 Statistika:" + Style.RESET_ALL)
                print(f"   Jami ulanishlar: {stats[0]['total_connections']}")
                print(f"   Aktiv ulanishlar: {stats[0]['active_connections']}")
                print(f"   Database hajmi: {stats[0]['total_db_size']}")
                print(f"   Jami tablelar: {stats[0]['total_tables']}")
            
        except Exception as e:
            print(Fore.RED + f"\n❌ Xato: {e}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _remove_database_url_ui(self):
        """URL o'chirish"""
        self._list_database_urls_ui()
        
        if self.url_manager.urls:
            name = input("\nO'chirish uchun URL nomi: ").strip()
            confirm = input(f"'{name}' ni o'chirishni tasdiqlaysizmi? [y/N]: ").strip().lower()
            
            if confirm == 'y':
                if self.url_manager.remove_url(name):
                    if self.current_db_url and self.current_db_url == self.url_manager.get_url(name):
                        self.current_db_url = None
                    print(Fore.GREEN + f"\n✅ URL '{name}' o'chirildi!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _edit_database_url_ui(self):
        """URL tahrirlash"""
        self._list_database_urls_ui()
        
        if self.url_manager.urls:
            name = input("\nTahrirlash uchun URL nomi: ").strip()
            url = self.url_manager.get_url(name)
            
            if url:
                print(f"\nHozirgi URL: {url.to_string(hide_password=True)}")
                print("\nYangi ma'lumotlarni kiriting (Enter - o'zgartirmaslik):")
                
                host = input(f"  Host [{url.host}]: ").strip() or url.host
                port = input(f"  Port [{url.port}]: ").strip() or str(url.port)
                database = input(f"  Database [{url.database}]: ").strip() or url.database
                username = input(f"  Username [{url.username}]: ").strip() or url.username
                
                change_password = input("  Parolni o'zgartirishmi? [y/N]: ").strip().lower()
                password = url.password
                if change_password == 'y':
                    password = getpass.getpass("  Yangi password: ").strip()
                
                sslmode = input(f"  SSL mode [{url.params.get('sslmode', 'prefer')}]: ").strip() or url.params.get('sslmode', 'prefer')
                
                # Yangi URL yaratish
                new_url = DatabaseURL(
                    username=username,
                    password=password,
                    host=host,
                    port=int(port),
                    database=database,
                    params={'sslmode': sslmode}
                )
                
                # Eski URL ni o'chirib, yangisini qo'shish
                self.url_manager.remove_url(name)
                self.url_manager.add_url(name, new_url)
                
                if self.current_db_url and self.current_db_url == url:
                    self.current_db_url = new_url
                
                print(Fore.GREEN + f"\n✅ URL '{name}' tahrirlandi!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _remove_deployment_ui(self):
        """Deployment o'chirish"""
        self._list_deployments_ui()
        
        if self.deployment_manager.deployments:
            name = input("\nO'chirish uchun deployment nomi: ").strip()
            confirm = input(f"'{name}' ni o'chirishni tasdiqlaysizmi? [y/N]: ").strip().lower()
            
            if confirm == 'y':
                if self.deployment_manager.remove_deployment(name):
                    if self.current_deployment == name:
                        self.current_deployment = None
                    print(Fore.GREEN + f"\n✅ Deployment '{name}' o'chirildi!" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _deployment_logs_ui(self):
        """Deployment loglari"""
        if not self.current_deployment:
            print(Fore.RED + "\n❌ Avval deployment tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        self.clear_screen()
        print(Fore.CYAN + f"\n📜 DEPLOYMENT LOGS: {self.current_deployment}" + Style.RESET_ALL)
        print(Fore.CYAN + "-" * 50 + Style.RESET_ALL)
        
        try:
            with open(config.LOG_FILE, 'r') as f:
                lines = f.readlines()[-30:]  # Oxirgi 30 qator
                
                for line in lines:
                    if self.current_deployment in line:
                        if 'ERROR' in line:
                            print(Fore.RED + line.strip() + Style.RESET_ALL)
                        elif 'WARNING' in line:
                            print(Fore.YELLOW + line.strip() + Style.RESET_ALL)
                        elif 'SUCCESS' in line or '✅' in line:
                            print(Fore.GREEN + line.strip() + Style.RESET_ALL)
                        else:
                            print(line.strip())
        except Exception as e:
            print(Fore.RED + f"Loglarni o'qishda xato: {e}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _export_deployment_config_ui(self):
        """Deployment config export"""
        if not self.current_deployment:
            print(Fore.RED + "\n❌ Avval deployment tanlang!" + Style.RESET_ALL)
            input("\nDavom etish uchun Enter bosing...")
            return
        
        deployment = self.deployment_manager.get_deployment(self.current_deployment)
        
        filename = input(f"Fayl nomi [{deployment.name}_config.json]: ").strip() or f"{deployment.name}_config.json"
        
        config_data = {
            'name': deployment.name,
            'environment': deployment.environment.value,
            'deployment_type': deployment.deployment_type.value,
            'connection_mode': deployment.connection_mode.value,
            'database_url': deployment.database_url.to_string(),
            'pool_size': deployment.pool_size,
            'max_overflow': deployment.max_overflow,
            'timeout': deployment.timeout,
            'ssl_enabled': deployment.ssl_enabled,
            'backup_enabled': deployment.backup_enabled,
            'monitoring_enabled': deployment.monitoring_enabled,
            'created_at': deployment.created_at.isoformat(),
            'updated_at': deployment.updated_at.isoformat(),
            'metadata': deployment.metadata
        }
        
        with open(filename, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        print(Fore.GREEN + f"\n✅ Deployment konfiguratsiyasi eksport qilindi: {filename}" + Style.RESET_ALL)
        
        input("\nDavom etish uchun Enter bosing...")
    
    def _exit_ui(self):
        """Dasturdan chiqish"""
        print(Fore.GREEN + "\n✅ PostgreSQL Enterprise Manager to'xtatildi!" + Style.RESET_ALL)
        print(Fore.CYAN + "   Xayr! 👋" + Style.RESET_ALL)
        self.running = False

# ============================================================================
# ASOSIY DASTUR
# ============================================================================

def main():
    """Dasturni ishga tushirish"""
    
    # Root huquqini tekshirish
    if os.geteuid() != 0:
        print(Fore.RED + "❌ Bu dastur root huquqi bilan ishga tushirilishi kerak!" + Style.RESET_ALL)
        print("   sudo python3 postgres_enterprise.py")
        sys.exit(1)
    
    try:
        # Interfeysni ishga tushirish
        ui = PostgreSQLEnterpriseUI()
        ui.run()
        
    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n\n⚠️ Dastur to'xtatildi!" + Style.RESET_ALL)
    except Exception as e:
        print(Fore.RED + f"\n❌ Kutilmagan xato: {e}" + Style.RESET_ALL)
        logger.error(f"Kutilmagan xato: {e}")

if __name__ == "__main__":
    main()