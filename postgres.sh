#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ============================================================================
# PostgreSQL Enterprise Management Script
# Version: 2.1.0
# Author: DevOps Team
# Description: Production-grade PostgreSQL automation with security hardening
# ============================================================================

# ------------------------------
# Global Configuration
# ------------------------------
readonly SCRIPT_NAME="postgres.sh"
readonly SCRIPT_VERSION="2.1.0"
readonly LOG_DIR="/var/log/postgres-enterprise"
readonly LOG_FILE="${LOG_DIR}/postgres-$(date +%Y%m%d).log"
readonly CONFIG_BACKUP_DIR="/etc/postgresql/backups"
readonly PG_HBA_CONF_PATH=""
readonly PG_CONF_PATH=""
readonly COLOR_RED='\033[0;31m'
readonly COLOR_GREEN='\033[0;32m'
readonly COLOR_YELLOW='\033[1;33m'
readonly COLOR_BLUE='\033[0;34m'
readonly COLOR_PURPLE='\033[0;35m'
readonly COLOR_CYAN='\033[0;36m'
readonly COLOR_WHITE='\033[1;37m'
readonly COLOR_RESET='\033[0m'

# ------------------------------
# Initialization
# ------------------------------
init_environment() {
    # Create necessary directories
    mkdir -p "${LOG_DIR}"
    mkdir -p "${CONFIG_BACKUP_DIR}"
    
    # Initialize log file
    touch "${LOG_FILE}"
    
    # Set secure umask
    umask 027
    
    # Check if running as root
    if [[ $EUID -eq 0 ]]; then
        log "WARNING" "Running as root is not recommended. Consider using a dedicated postgres user."
    fi
}

# ------------------------------
# Logging System
# ------------------------------
log() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${timestamp} [${level}] ${message}" >> "${LOG_FILE}"
    
    case "${level}" in
        "ERROR")   echo -e "${COLOR_RED}[ERROR]${COLOR_RESET} ${message}" >&2 ;;
        "WARNING") echo -e "${COLOR_YELLOW}[WARNING]${COLOR_RESET} ${message}" ;;
        "SUCCESS") echo -e "${COLOR_GREEN}[SUCCESS]${COLOR_RESET} ${message}" ;;
        "INFO")    echo -e "${COLOR_CYAN}[INFO]${COLOR_RESET} ${message}" ;;
        *)         echo -e "${message}" ;;
    esac
}

# ------------------------------
# PostgreSQL Detection
# ------------------------------
detect_postgres_version() {
    local version=""
    
    if command -v psql &> /dev/null; then
        version=$(psql --version | head -n1 | grep -oP '\d+' | head -1)
        log "INFO" "PostgreSQL ${version} detected"
    else
        # Check installed but not in PATH
        if [[ -d "/usr/lib/postgresql" ]]; then
            version=$(ls /usr/lib/postgresql/ 2>/dev/null | sort -V | tail -n1)
        fi
    fi
    
    echo "${version}"
}

detect_pg_dir() {
    local version="$1"
    local pg_dir=""
    
    if [[ -n "${version}" ]]; then
        if [[ -d "/etc/postgresql/${version}/main" ]]; then
            pg_dir="/etc/postgresql/${version}/main"
        elif [[ -d "/var/lib/postgresql/${version}/main" ]]; then
            pg_dir="/var/lib/postgresql/${version}/main"
        fi
    fi
    
    echo "${pg_dir}"
}

is_postgres_installed() {
    local version=$(detect_postgres_version)
    if [[ -n "${version}" ]] && systemctl is-active --quiet postgresql; then
        return 0
    fi
    return 1
}

# ------------------------------
# Installation Functions
# ------------------------------
install_postgresql() {
    log "INFO" "Starting PostgreSQL installation..."
    
    # Add PostgreSQL repository
    if ! grep -q "postgresql.org" /etc/apt/sources.list.d/* 2>/dev/null; then
        log "INFO" "Adding PostgreSQL official repository..."
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
        echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
    fi
    
    # Update package list
    apt-get update -qq
    
    # Install PostgreSQL
    apt-get install -y postgresql postgresql-contrib postgresql-client \
        postgresql-common libpq-dev python3-psycopg2
        
    # Enable and start service
    systemctl enable postgresql
    systemctl start postgresql
    
    # Verify installation
    if systemctl is-active --quiet postgresql; then
        log "SUCCESS" "PostgreSQL installed and running successfully"
        
        # Set postgres user password
        sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD '$(generate_strong_password)';"
        
        return 0
    else
        log "ERROR" "PostgreSQL installation failed"
        return 1
    fi
}

generate_strong_password() {
    openssl rand -base64 32 | tr -d /=+ | cut -c1-32
}

# ------------------------------
# Configuration Management
# ------------------------------
backup_config() {
    local version="$1"
    local config_dir="/etc/postgresql/${version}/main"
    local backup_file="${CONFIG_BACKUP_DIR}/postgresql-${version}-$(date +%Y%m%d_%H%M%S).tar.gz"
    
    if [[ -d "${config_dir}" ]]; then
        tar -czf "${backup_file}" "${config_dir}" 2>/dev/null
        log "SUCCESS" "Configuration backed up to ${backup_file}"
        echo "${backup_file}"
    else
        log "ERROR" "Config directory not found: ${config_dir}"
        return 1
    fi
}

configure_postgresql_conf() {
    local version="$1"
    local mode="$2"
    local config_file="/etc/postgresql/${version}/main/postgresql.conf"
    
    # Backup before modification
    backup_config "${version}"
    
    # Create temp file for modifications
    local temp_file=$(mktemp)
    
    # Base configuration
    cat > "${temp_file}" << EOF
# PostgreSQL Enterprise Configuration
# Generated by ${SCRIPT_NAME} v${SCRIPT_VERSION}
# Date: $(date)

# Connection Settings
listen_addresses = '${mode}'
port = 5432
max_connections = 100
superuser_reserved_connections = 3

# Memory Settings
shared_buffers = 256MB
effective_cache_size = 768MB
work_mem = 4MB
maintenance_work_mem = 64MB

# Write Ahead Log
wal_level = replica
wal_buffers = 16MB
checkpoint_completion_target = 0.9
min_wal_size = 1GB
max_wal_size = 4GB

# Query Tuning
random_page_cost = 1.1
effective_io_concurrency = 200
max_parallel_workers_per_gather = 2
max_parallel_workers = 4

# Logging
log_destination = 'stderr'
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'
log_rotation_age = 1d
log_rotation_size = 100MB
log_min_duration_statement = 1000
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
log_temp_files = 0
log_autovacuum_min_duration = 0

# Autovacuum
autovacuum = on
autovacuum_naptime = 1min
autovacuum_vacuum_threshold = 50
autovacuum_analyze_threshold = 50
autovacuum_vacuum_scale_factor = 0.2
autovacuum_analyze_scale_factor = 0.1
autovacuum_freeze_max_age = 500000000

# Statistics
track_activities = on
track_counts = on
track_io_timing = on
track_functions = all
track_activity_query_size = 2048

# SSL Settings
ssl = on
ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem'
ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key'

# Security
password_encryption = scram-sha-256
EOF

    # Apply remote mode specific settings
    if [[ "${mode}" == "remote" ]]; then
        cat >> "${temp_file}" << EOF

# Remote Mode Specific Settings
listen_addresses = '*'
max_connections = 200
shared_buffers = 512MB
effective_cache_size = 1.5GB
work_mem = 8MB
maintenance_work_mem = 128MB
EOF
    fi
    
    # Backup original and apply new config
    cp "${config_file}" "${config_file}.backup"
    cp "${temp_file}" "${config_file}"
    rm "${temp_file}"
    
    log "SUCCESS" "PostgreSQL configuration updated for ${mode} mode"
}

configure_pg_hba() {
    local version="$1"
    local mode="$2"
    local hba_file="/etc/postgresql/${version}/main/pg_hba.conf"
    
    # Backup
    cp "${hba_file}" "${hba_file}.backup.$(date +%s)"
    
    # Create new pg_hba.conf
    local temp_file=$(mktemp)
    
    cat > "${temp_file}" << EOF
# PostgreSQL Client Authentication Configuration File
# Generated by ${SCRIPT_NAME} v${SCRIPT_VERSION}
# Date: $(date)

# TYPE  DATABASE        USER            ADDRESS                 METHOD

# Local connections
local   all             all                                     scram-sha-256
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             ::1/128                 scram-sha-256

EOF

    if [[ "${mode}" == "remote" ]]; then
        cat >> "${temp_file}" << EOF

# Remote connections - Secure configuration
# Private network ranges
host    all             all             10.0.0.0/8              scram-sha-256
host    all             all             172.16.0.0/12          scram-sha-256
host    all             all             192.168.0.0/16         scram-sha-256

# Optional: VPN networks
# host    all          all             10.10.0.0/16            scram-sha-256

# Reject all other connections
host    all             all             0.0.0.0/0               reject
host    all             all             ::/0                    reject
EOF
    fi
    
    cp "${temp_file}" "${hba_file}"
    rm "${temp_file}"
    
    log "SUCCESS" "pg_hba.conf configured for ${mode} mode"
    
    # Reload configuration
    systemctl reload postgresql
}

# ------------------------------
# Database Management Functions
# ------------------------------
create_database() {
    local db_name="$1"
    local owner="$2"
    
    read -p "Enter database name: " db_name
    read -p "Enter owner (default: postgres): " owner
    owner=${owner:-postgres}
    
    sudo -u postgres psql -c "CREATE DATABASE ${db_name} OWNER ${owner};" 2>/dev/null
    
    if [[ $? -eq 0 ]]; then
        log "SUCCESS" "Database ${db_name} created with owner ${owner}"
    else
        log "ERROR" "Failed to create database ${db_name}"
    fi
}

create_user() {
    local username="$1"
    local password="$2"
    
    read -p "Enter username: " username
    read -s -p "Enter password: " password
    echo
    
    # Validate password strength
    if [[ ${#password} -lt 12 ]]; then
        log "ERROR" "Password must be at least 12 characters long"
        return 1
    fi
    
    if [[ ! "$password" =~ [A-Z] ]] || [[ ! "$password" =~ [a-z] ]] || \
       [[ ! "$password" =~ [0-9] ]] || [[ ! "$password" =~ [^a-zA-Z0-9] ]]; then
        log "ERROR" "Password must contain uppercase, lowercase, number, and special character"
        return 1
    fi
    
    sudo -u postgres psql -c "CREATE USER ${username} WITH PASSWORD '${password}';"
    
    if [[ $? -eq 0 ]]; then
        log "SUCCESS" "User ${username} created successfully"
    else
        log "ERROR" "Failed to create user ${username}"
    fi
}

assign_role() {
    local username="$1"
    local role_type="$2"
    
    echo "Select role type:"
    echo "1) Superuser"
    echo "2) Read-only"
    echo "3) Read-write"
    echo "4) Custom"
    
    read -p "Choice [1-4]: " choice
    
    case $choice in
        1) sudo -u postgres psql -c "ALTER USER ${username} WITH SUPERUSER;" ;;
        2) 
            sudo -u postgres psql -c "ALTER USER ${username} WITH NOSUPERUSER;"
            sudo -u postgres psql -c "GRANT CONNECT ON DATABASE ${DB_NAME} TO ${username};"
            sudo -u postgres psql -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${username};"
            ;;
        3)
            sudo -u postgres psql -c "ALTER USER ${username} WITH NOSUPERUSER;"
            sudo -u postgres psql -c "GRANT CONNECT, CREATE ON DATABASE ${DB_NAME} TO ${username};"
            sudo -u postgres psql -c "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${username};"
            ;;
        4)
            read -p "Enter custom privileges: " custom_privs
            sudo -u postgres psql -c "${custom_privs}"
            ;;
    esac
    
    log "SUCCESS" "Role assigned to ${username}"
}

# ------------------------------
# Backup and Restore
# ------------------------------
backup_database() {
    local db_name="$1"
    local backup_path="/var/backups/postgresql"
    
    mkdir -p "${backup_path}"
    
    local backup_file="${backup_path}/${db_name}_$(date +%Y%m%d_%H%M%S).sql"
    
    sudo -u postgres pg_dump -d "${db_name}" --clean --if-exists --no-owner \
        --no-privileges --format=custom > "${backup_file}.dump"
    
    # Also create plain SQL backup
    sudo -u postgres pg_dump -d "${db_name}" > "${backup_file}"
    
    # Compress
    gzip "${backup_file}"
    
    log "SUCCESS" "Database ${db_name} backed up to ${backup_file}.gz"
    echo "${backup_file}.gz"
}

restore_database() {
    local db_name="$1"
    local backup_file="$2"
    
    read -p "Enter backup file path: " backup_file
    
    if [[ ! -f "${backup_file}" ]]; then
        log "ERROR" "Backup file not found"
        return 1
    fi
    
    # Drop connections
    sudo -u postgres psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${db_name}' AND pid <> pg_backend_pid();"
    
    # Drop and recreate database
    sudo -u postgres psql -c "DROP DATABASE IF EXISTS ${db_name};"
    sudo -u postgres psql -c "CREATE DATABASE ${db_name};"
    
    # Restore
    if [[ "${backup_file}" == *.dump ]]; then
        sudo -u postgres pg_restore -d "${db_name}" "${backup_file}"
    else
        sudo -u postgres psql -d "${db_name}" -f "${backup_file}"
    fi
    
    log "SUCCESS" "Database ${db_name} restored from ${backup_file}"
}

# ------------------------------
# Import SQL
# ------------------------------
import_sql() {
    local db_name="$1"
    local sql_file="$2"
    
    read -p "Enter database name: " db_name
    read -p "Enter SQL file path: " sql_file
    
    if [[ ! -f "${sql_file}" ]]; then
        log "ERROR" "SQL file not found"
        return 1
    fi
    
    # Validate SQL syntax
    sudo -u postgres psql -d "${db_name}" -c "SET client_min_messages='ERROR';" \
        -f "${sql_file}" --set ON_ERROR_STOP=on -o /dev/null 2>/dev/null
    
    if [[ $? -eq 0 ]]; then
        # Execute import
        sudo -u postgres psql -d "${db_name}" -f "${sql_file}"
        log "SUCCESS" "SQL file imported successfully to ${db_name}"
    else
        log "ERROR" "SQL syntax validation failed"
        return 1
    fi
}

# ------------------------------
# Monitoring Functions
# ------------------------------
show_active_connections() {
    sudo -u postgres psql -x -c "
        SELECT 
            pid,
            usename,
            application_name,
            client_addr,
            state,
            query,
            age(now(), query_start) as running_time,
            wait_event_type || ': ' || wait_event as waiting
        FROM pg_stat_activity 
        WHERE state = 'active' 
        AND pid <> pg_backend_pid()
        ORDER BY query_start DESC;"
}

show_database_size() {
    sudo -u postgres psql -c "
        SELECT 
            pg_database.datname,
            pg_size_pretty(pg_database_size(pg_database.datname)) as size,
            pg_database_size(pg_database.datname) as bytes
        FROM pg_database
        ORDER BY bytes DESC;"
}

show_table_sizes() {
    read -p "Enter database name: " db_name
    
    sudo -u postgres psql -d "${db_name}" -c "
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                          pg_relation_size(schemaname||'.'||tablename)) as indexes,
            n_live_tup as rows
        FROM pg_tables t
        JOIN pg_stat_user_tables s ON t.tablename = s.relname
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT 20;"
}

show_performance_metrics() {
    sudo -u postgres psql -c "
        -- Cache hit ratio
        SELECT 
            'cache_hit' as name,
            round(sum(heap_blks_hit) * 100 / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0), 2) as ratio
        FROM pg_statio_user_tables
        
        UNION ALL
        
        -- Index usage
        SELECT 
            'index_usage',
            round(sum(idx_tup_fetch) * 100 / nullif(sum(idx_tup_fetch) + sum(idx_tup_read), 0), 2)
        FROM pg_stat_user_tables
        
        UNION ALL
        
        -- Active connections
        SELECT 
            'active_connections',
            count(*)::numeric
        FROM pg_stat_activity
        WHERE state = 'active';
        
        -- Slow queries
        SELECT 
            query,
            calls,
            total_time / calls as avg_time,
            rows / calls as avg_rows,
            mean_time
        FROM pg_stat_statements
        ORDER BY total_time DESC
        LIMIT 10;"
}

# ------------------------------
# Firewall Management
# ------------------------------
open_firewall_port() {
    if command -v ufw &> /dev/null; then
        ufw allow 5432/tcp comment 'PostgreSQL'
        log "SUCCESS" "Firewall port 5432 opened"
    elif command -v firewall-cmd &> /dev/null; then
        firewall-cmd --permanent --add-port=5432/tcp
        firewall-cmd --reload
        log "SUCCESS" "Firewall port 5432 opened"
    else
        log "WARNING" "No supported firewall found. Please configure manually."
    fi
}

# ------------------------------
# Reset Configuration
# ------------------------------
reset_configuration() {
    local version="$1"
    
    read -p "Are you sure you want to reset PostgreSQL configuration? [y/N]: " confirm
    
    if [[ "${confirm,,}" == "y" ]]; then
        # Restore from backup
        local latest_backup=$(ls -t ${CONFIG_BACKUP_DIR}/postgresql-${version}-*.tar.gz 2>/dev/null | head -n1)
        
        if [[ -n "${latest_backup}" ]]; then
            tar -xzf "${latest_backup}" -C /
            systemctl restart postgresql
            log "SUCCESS" "Configuration reset to backup: ${latest_backup}"
        else
            # Reinstall configuration
            apt-get install --reinstall -y postgresql-${version}
            log "INFO" "Configuration reinstalled from packages"
        fi
    fi
}

# ------------------------------
# Interactive Menu
# ------------------------------
show_header() {
    clear
    echo -e "${COLOR_CYAN}═══════════════════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo -e "${COLOR_GREEN}   ██████╗ ███████╗███████╗████████╗ ██████╗ ██████╗ ███████╗███████╗██╗${COLOR_RESET}"
    echo -e "${COLOR_GREEN}   ██╔══██╗██╔════╝██╔════╝╚══██╔══╝██╔════╝ ██╔══██╗██╔════╝██╔════╝██║${COLOR_RESET}"
    echo -e "${COLOR_GREEN}   ██████╔╝███████╗█████╗     ██║   ██║  ███╗██████╔╝█████╗  █████╗  ██║${COLOR_RESET}"
    echo -e "${COLOR_GREEN}   ██╔═══╝ ╚════██║██╔══╝     ██║   ██║   ██║██╔══██╗██╔══╝  ██╔══╝  ╚═╝${COLOR_RESET}"
    echo -e "${COLOR_GREEN}   ██║     ███████║███████╗   ██║   ╚██████╔╝██║  ██║███████╗███████╗██╗${COLOR_RESET}"
    echo -e "${COLOR_GREEN}   ╚═╝     ╚══════╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝${COLOR_RESET}"
    echo -e "${COLOR_CYAN}═══════════════════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo -e "${COLOR_WHITE}                    Enterprise PostgreSQL Management Script v${SCRIPT_VERSION}${COLOR_RESET}"
    echo -e "${COLOR_CYAN}═══════════════════════════════════════════════════════════════════════════${COLOR_RESET}"
    echo
}

show_menu() {
    show_header
    
    local version=$(detect_postgres_version)
    local installed=""
    local status_color="${COLOR_RED}"
    
    if is_postgres_installed; then
        installed="✓ PostgreSQL ${version} (running)"
        status_color="${COLOR_GREEN}"
    else
        installed="✗ PostgreSQL (not installed)"
        status_color="${COLOR_RED}"
    fi
    
    echo -e "${status_color}${installed}${COLOR_RESET}"
    echo
    echo -e "${COLOR_YELLOW}INSTALLATION & CONFIGURATION${COLOR_RESET}"
    echo " 1) Install PostgreSQL                        11) Import SQL file"
    echo " 2) Configure Local Mode                      12) Export/Backup Database"
    echo " 3) Configure Remote Mode (Secure)            13) Restore Backup"
    echo " 4) Configure Remote Mode (Open)              14) Drop Database"
    echo " 5) Open Firewall Port                        15) Drop User"
    echo
    echo -e "${COLOR_YELLOW}DATABASE MANAGEMENT${COLOR_RESET}"
    echo " 6) Create Database                           16) List Databases"
    echo " 7) Create User                               17) List Users"
    echo " 8) Assign Role/Permissions                   18) Reset Configuration"
    echo " 9) Grant Privileges"
    echo "10) Enable Password Encryption"
    echo
    echo -e "${COLOR_YELLOW}MONITORING & PERFORMANCE${COLOR_RESET}"
    echo "19) Show Active Connections                   23) Show Database Size"
    echo "20) Show Table Sizes                          24) Show Uptime"
    echo "21) Performance Monitoring                    25) Show Replication Status"
    echo "22) Show Connection Statistics                26) Show Memory Usage"
    echo
    echo -e "${COLOR_YELLOW}SECURITY & MAINTENANCE${COLOR_RESET}"
    echo "27) Secure Production Mode (IP Restricted)    29) Generate Python Client Examples"
    echo "28) Run Security Audit                        30) Exit"
    echo
    echo -e "${COLOR_CYAN}═══════════════════════════════════════════════════════════════════════════${COLOR_RESET}"
}

process_choice() {
    local choice="$1"
    local version=$(detect_postgres_version)
    
    case $choice in
        1) install_postgresql ;;
        2) 
            if is_postgres_installed; then
                configure_postgresql_conf "${version}" "localhost"
                configure_pg_hba "${version}" "local"
            else
                log "ERROR" "PostgreSQL not installed. Please install first."
            fi
            ;;
        3)
            if is_postgres_installed; then
                configure_postgresql_conf "${version}" "remote"
                configure_pg_hba "${version}" "remote"
            else
                log "ERROR" "PostgreSQL not installed. Please install first."
            fi
            ;;
        4)
            if is_postgres_installed; then
                configure_postgresql_conf "${version}" "remote"
                # Less restrictive remote config
                local temp_file=$(mktemp)
                cat > "${temp_file}" << EOF
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     scram-sha-256
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             ::1/128                 scram-sha-256
host    all             all             0.0.0.0/0               scram-sha-256
EOF
                cp "${temp_file}" "/etc/postgresql/${version}/main/pg_hba.conf"
                rm "${temp_file}"
                systemctl reload postgresql
                log "SUCCESS" "Open remote access configured"
            fi
            ;;
        5) open_firewall_port ;;
        6) create_database ;;
        7) create_user ;;
        8) assign_role ;;
        9) grant_privileges ;;
        10) enable_password_encryption ;;
        11) import_sql ;;
        12) backup_database ;;
        13) restore_database ;;
        14) drop_database ;;
        15) drop_user ;;
        16) list_databases ;;
        17) list_users ;;
        18) reset_configuration "${version}" ;;
        19) show_active_connections ;;
        20) show_table_sizes ;;
        21) show_performance_metrics ;;
        22) show_connection_stats ;;
        23) show_database_size ;;
        24) show_uptime ;;
        25) show_replication_status ;;
        26) show_memory_usage ;;
        27) secure_production_mode ;;
        28) run_security_audit ;;
        29) generate_python_examples ;;
        30) 
            log "INFO" "Exiting PostgreSQL Management Script"
            exit 0
            ;;
        *)
            log "ERROR" "Invalid option"
            ;;
    esac
    
    read -p "Press Enter to continue..."
}

# ------------------------------
# Additional Helper Functions
# ------------------------------
list_databases() {
    sudo -u postgres psql -l
}

list_users() {
    sudo -u postgres psql -c "SELECT usename, usesuper, usecreatedb, valuntil FROM pg_user;"
}

drop_database() {
    read -p "Enter database name to drop: " db_name
    read -p "Are you sure? This cannot be undone! [y/N]: " confirm
    
    if [[ "${confirm,,}" == "y" ]]; then
        # Terminate connections
        sudo -u postgres psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${db_name}';"
        sudo -u postgres psql -c "DROP DATABASE ${db_name};"
        log "SUCCESS" "Database ${db_name} dropped"
    fi
}

drop_user() {
    read -p "Enter username to drop: " username
    read -p "Are you sure? [y/N]: " confirm
    
    if [[ "${confirm,,}" == "y" ]]; then
        sudo -u postgres psql -c "DROP USER ${username};"
        log "SUCCESS" "User ${username} dropped"
    fi
}

show_uptime() {
    sudo -u postgres psql -c "SELECT pg_postmaster_start_time() as server_start, 
                                     current_timestamp - pg_postmaster_start_time() as uptime;"
}

show_replication_status() {
    sudo -u postgres psql -c "SELECT * FROM pg_stat_replication;"
    sudo -u postgres psql -c "SELECT * FROM pg_stat_subscription;"
}

show_memory_usage() {
    echo "PostgreSQL Memory Usage:"
    ps aux | grep postgres | grep -v grep
    echo
    free -h
}

secure_production_mode() {
    local version=$(detect_postgres_version)
    
    log "INFO" "Applying production security hardening..."
    
    # Restrict pg_hba.conf to specific IPs
    read -p "Enter allowed IP range (e.g., 10.0.0.0/24): " ip_range
    
    local hba_file="/etc/postgresql/${version}/main/pg_hba.conf"
    cp "${hba_file}" "${hba_file}.prod.backup"
    
    cat > "${hba_file}" << EOF
# Production Security Configuration
local   all             all                                     scram-sha-256
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             ${ip_range}             scram-sha-256
host    all             all             ::1/128                 reject
host    all             all             0.0.0.0/0               reject
EOF
    
    # Additional security settings
    local conf_file="/etc/postgresql/${version}/main/postgresql.conf"
    echo "ssl = on" >> "${conf_file}"
    echo "ssl_prefer_server_ciphers = on" >> "${conf_file}"
    echo "ssl_min_protocol_version = 'TLSv1.2'" >> "${conf_file}"
    
    systemctl restart postgresql
    log "SUCCESS" "Production security mode enabled"
}

run_security_audit() {
    log "INFO" "Running security audit..."
    
    echo "=== PostgreSQL Security Audit ==="
    echo
    
    echo "1. Authentication Method:"
    grep -E "host.*all.*all" /etc/postgresql/*/main/pg_hba.conf
    
    echo
    echo "2. SSL Configuration:"
    sudo -u postgres psql -c "SHOW ssl;"
    sudo -u postgres psql -c "SHOW ssl_ciphers;"
    
    echo
    echo "3. Password Encryption:"
    sudo -u postgres psql -c "SHOW password_encryption;"
    
    echo
    echo "4. Superusers:"
    sudo -u postgres psql -c "SELECT usename FROM pg_user WHERE usesuper IS TRUE;"
    
    echo
    echo "5. Empty Passwords:"
    sudo -u postgres psql -c "SELECT usename FROM pg_shadow WHERE passwd IS NULL OR passwd = '';"
    
    echo
    echo "6. Database Sizes:"
    show_database_size
    
    echo
    echo "7. Failed Login Attempts:"
    grep "failed" /var/log/postgresql/postgresql-*.log 2>/dev/null | tail -20
}

generate_python_examples() {
    cat > /tmp/postgres_local_example.py << 'EOF'
#!/usr/bin/env python3
"""
PostgreSQL Local Connection Example
Enterprise-grade connection handler with security best practices
"""
import os
import psycopg2
from psycopg2 import pool, extras, errors
from contextlib import contextmanager
import logging
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgreSQLConnection:
    """Production PostgreSQL connection manager"""
    
    def __init__(self, dbname='postgres', user='postgres', 
                 password=None, host='localhost', port=5432,
                 min_conn=1, max_conn=10):
        
        self.dbname = dbname
        self.user = user
        self.password = password or os.environ.get('PGPASSWORD')
        self.host = host
        self.port = port
        self.connection_pool = None
        self.max_retries = 3
        
        if not self.password:
            raise ValueError("Password must be provided or set in PGPASSWORD environment variable")
        
        self._create_pool(min_conn, max_conn)
    
    def _create_pool(self, min_conn, max_conn):
        """Create connection pool with secure settings"""
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                min_conn,
                max_conn,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                sslmode='require',  # Force SSL
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
                options='-c statement_timeout=30s'
            )
            logger.info(f"Connection pool created with {min_conn}-{max_conn} connections")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor with automatic cleanup"""
        conn = None
        try:
            conn = self.connection_pool.getconn()
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
            yield cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.connection_pool.putconn(conn)
    
    def execute_with_retry(self, query, params=None):
        """Execute query with automatic retry logic"""
        for attempt in range(self.max_retries):
            try:
                with self.get_cursor() as cursor:
                    cursor.execute(query, params)
                    if cursor.description:  # SELECT query
                        return cursor.fetchall()
                    return None
            except (errors.OperationalError, errors.InterfaceError) as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    raise
                sleep(2 ** attempt)  # Exponential backoff
    
    def close_all(self):
        """Properly close all connections"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("All connections closed")

# Example usage
if __name__ == "__main__":
    # Use environment variables for credentials
    db = PostgreSQLConnection(
        dbname=os.environ.get('PGDATABASE', 'postgres'),
        user=os.environ.get('PGUSER', 'postgres'),
        host=os.environ.get('PGHOST', 'localhost')
    )
    
    try:
        # Test connection
        result = db.execute_with_retry("SELECT version();")
        print(f"PostgreSQL version: {result[0]['version']}")
        
        # Get database size
        result = db.execute_with_retry("""
            SELECT pg_database.datname, 
                   pg_size_pretty(pg_database_size(pg_database.datname)) as size
            FROM pg_database
            WHERE datname = %s
        """, (os.environ.get('PGDATABASE', 'postgres'),))
        print(f"Database size: {result[0]['size']}")
        
    finally:
        db.close_all()
EOF

    cat > /tmp/postgres_remote_example.py << 'EOF'
#!/usr/bin/env python3
"""
PostgreSQL Remote Connection Example
With SSL, connection pooling, and production error handling
"""
import os
import ssl
import psycopg2
from psycopg2 import pool, sql
import logging
from urllib.parse import urlparse
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RemotePostgreSQL:
    """Secure remote PostgreSQL connection handler"""
    
    def __init__(self, database_url=None):
        """Initialize with database URL or individual parameters"""
        if database_url:
            self._parse_database_url(database_url)
        else:
            self.host = os.environ.get('PGHOST_REMOTE')
            self.port = int(os.environ.get('PGPORT_REMOTE', 5432))
            self.dbname = os.environ.get('PGDATABASE_REMOTE')
            self.user = os.environ.get('PGUSER_REMOTE')
            self.password = os.environ.get('PGPASSWORD_REMOTE')
            self.sslmode = os.environ.get('PGSSLMODE', 'verify-full')
            self.sslrootcert = os.environ.get('PGSSLROOTCERT')
        
        self.connection_pool = None
        self._validate_config()
        self._create_pool()
    
    def _parse_database_url(self, url):
        """Parse PostgreSQL URL format"""
        parsed = urlparse(url)
        self.host = parsed.hostname
        self.port = parsed.port or 5432
        self.dbname = parsed.path[1:]  # Remove leading '/'
        self.user = parsed.username
        self.password = parsed.password
        self.sslmode = 'verify-full'
    
    def _validate_config(self):
        """Validate connection configuration"""
        required = ['host', 'dbname', 'user', 'password']
        for req in required:
            if not getattr(self, req):
                raise ValueError(f"Missing required configuration: {req}")
    
    def _create_pool(self):
        """Create connection pool with SSL"""
        conn_params = {
            'host': self.host,
            'port': self.port,
            'dbname': self.dbname,
            'user': self.user,
            'password': self.password,
            'sslmode': self.sslmode,
            'connect_timeout': 15,
            'options': '-c search_path=public -c statement_timeout=30s'
        }
        
        # Add SSL certificate if provided
        if self.sslrootcert:
            conn_params['sslrootcert'] = self.sslrootcert
        
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                1, 20, **conn_params
            )
            logger.info(f"Remote connection pool created for {self.host}")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    def execute_query(self, query, params=None, fetch=True):
        """Execute query with proper error handling"""
        conn = None
        try:
            conn = self.connection_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch and cur.description:
                    return cur.fetchall()
                conn.commit()
                return None
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def bulk_insert(self, table, data, batch_size=1000):
        """Perform bulk insert with batching"""
        if not data:
            return
        
        columns = data[0].keys()
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        
        conn = None
        try:
            conn = self.connection_pool.getconn()
            with conn.cursor() as cur:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    batch_params = [[row[col] for col in columns] for row in batch]
                    cur.executemany(insert_query, batch_params)
                    conn.commit()
                    logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} rows")
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Bulk insert failed: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)

# Production usage example
if __name__ == "__main__":
    # Load configuration from environment or vault
    config = {
        'host': os.environ.get('PGHOST_REMOTE'),
        'dbname': os.environ.get('PGDATABASE_REMOTE'),
        'user': os.environ.get('PGUSER_REMOTE'),
        'password': os.environ.get('PGPASSWORD_REMOTE')
    }
    
    db = RemotePostgreSQL()
    
    try:
        # Test remote connection
        result = db.execute_query("SELECT now() as server_time")
        print(f"Remote server time: {result[0][0]}")
        
        # Get connection statistics
        stats = db.execute_query("""
            SELECT count(*) as total,
                   count(*) FILTER (WHERE state = 'active') as active,
                   count(*) FILTER (WHERE state = 'idle') as idle,
                   count(DISTINCT client_addr) as unique_clients
            FROM pg_stat_activity
            WHERE datname = current_database()
        """)
        print(f"Connection stats: {stats[0]}")
        
    except Exception as e:
        logger.error(f"Remote connection failed: {e}")
        raise
EOF

    chmod 600 /tmp/postgres_local_example.py /tmp/postgres_remote_example.py
    
    log "SUCCESS" "Python client examples generated in /tmp/"
    echo "Location: /tmp/postgres_local_example.py"
    echo "Location: /tmp/postgres_remote_example.py"
}

# ------------------------------
# Main Execution
# ------------------------------
main() {
    init_environment
    
    while true; do
        show_menu
        read -p "Enter your choice [1-30]: " choice
        
        if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le 30 ]; then
            process_choice "$choice"
        else
            log "ERROR" "Please enter a valid option (1-30)"
            read -p "Press Enter to continue..."
        fi
    done
}

# Trap signals
trap 'log "INFO" "Script interrupted"; exit 1' INT TERM

# Run main function
main "$@"
