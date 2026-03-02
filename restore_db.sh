#!/usr/bin/env bash
set -euo pipefail

# =========================
# MediaCMS PostgreSQL Restore Helper
# - Demande un fichier backup (.dump ou .dump.gz)
# - Stoppe web/worker/frontend si présents
# - Copie le backup dans le conteneur DB
# - DROP DATABASE (FORCE) + CREATE
# - pg_restore
# =========================

# --- Paramètres par défaut (surcharge possibles via variables d'env) ---
DB_NAME="${DB_NAME:-mediacms}"
DB_USER="${DB_USER:-mediacms}"
BACKUP_DIR="${BACKUP_DIR:-./backup}"

# Détecte docker compose (v2 ou v1)
if command -v docker &>/dev/null && docker compose version &>/dev/null; then
  DC="docker compose"
elif command -v docker-compose &>/dev/null; then
  DC="docker-compose"
else
  echo "❌ Docker Compose introuvable." >&2
  exit 1
fi

# --- Helper logging ---
info(){ echo -e "👉 $*"; }
ok(){ echo -e "✅ $*"; }
warn(){ echo -e "⚠️  $*"; }
err(){ echo -e "❌ $*" >&2; }

# --- Vérifs préalables ---
command -v docker >/dev/null || { err "Docker requis."; exit 1; }

# --- Trouver le conteneur Postgres ---
detect_db_container() {
  # 1) Essayer un service 'db' du compose
  local cid
  set +e
  cid=$($DC ps -q db 2>/dev/null)
  set -e
  if [ -n "${cid:-}" ]; then
    docker ps --format '{{.Names}}' | grep -q "$(docker ps -q --no-trunc | grep "$cid")" 2>/dev/null || true
    echo "$cid"
    return 0
  fi

  # 2) Chercher un conteneur dont l'image contient 'postgres'
  cid=$(docker ps --format '{{.ID}} {{.Image}} {{.Names}}' | awk '/postgres/ {print $1; exit}')
  if [ -n "${cid:-}" ]; then
    echo "$cid"
    return 0
  fi

  # 3) En dernier recours, demander à l’utilisateur
  warn "Conteneur PostgreSQL non détecté automatiquement."
  read -r -p "Nom/ID du conteneur DB (ex: mediacms_db_1 ou a1b2c3...): " cid
  echo "$cid"
}

DB_CONTAINER="$(detect_db_container)"
[ -n "$DB_CONTAINER" ] || { err "Impossible d’identifier le conteneur DB."; exit 1; }

# --- Lister les backups disponibles ---
if [ -d "$BACKUP_DIR" ]; then
  echo "📦 Backups trouvés dans $BACKUP_DIR :"
  ls -1 "$BACKUP_DIR" | sed 's/^/  - /' || true
else
  warn "Le dossier $BACKUP_DIR n’existe pas. Je continue, tu peux donner un chemin absolu."
fi

# --- Demander le fichier ---
read -r -p "Nom du fichier de sauvegarde à restaurer (avec chemin si nécessaire) : " BACKUP_INPUT
BACKUP_FILE="$BACKUP_INPUT"

# Si l’utilisateur a donné juste un nom de fichier, cherche-le dans BACKUP_DIR
if [ ! -f "$BACKUP_FILE" ] && [ -f "$BACKUP_DIR/$BACKUP_FILE" ]; then
  BACKUP_FILE="$BACKUP_DIR/$BACKUP_FILE"
fi

[ -f "$BACKUP_FILE" ] || { err "Fichier $BACKUP_FILE introuvable."; exit 1; }

# Confirmation
echo
warn "ATTENTION : la base \"$DB_NAME\" sera supprimée puis recréée."
read -r -p "Continuer ? (yes/NO) " yn
[ "${yn:-}" = "yes" ] || { err "Annulé."; exit 1; }

# --- Stopper services applicatifs si présents ---
info "Arrêt des services applicatifs (si existants) : web, worker, frontend…"
set +e
$DC stop web worker frontend >/dev/null 2>&1
set -e
ok "Services arrêtés (ou inexistants)."

# --- Copier le backup dans le conteneur ---
TARGET_IN_CONTAINER="/tmp/$(basename "$BACKUP_FILE")"
info "Copie du backup dans le conteneur $DB_CONTAINER → $TARGET_IN_CONTAINER"
docker cp "$BACKUP_FILE" "$DB_CONTAINER:$TARGET_IN_CONTAINER"
ok "Backup copié."

# --- Fonctions utilitaires dans le conteneur ---
psql_c(){ docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$1" -v ON_ERROR_STOP=1 -c "$2"; }
sh_in(){ docker exec -i "$DB_CONTAINER" bash -lc "$*"; }

# --- Drop + Create DB ---
info "Suppression de la base \"$DB_NAME\" (FORCE si possible)…"
set +e
psql_c postgres "DROP DATABASE IF EXISTS $DB_NAME WITH (FORCE);" >/dev/null 2>&1
DROP_RC=$?
set -e

if [ $DROP_RC -ne 0 ]; then
  warn "Le DROP WITH (FORCE) a échoué. Tentative de fermeture des connexions, puis DROP…"
  # Tuer les connexions, puis DROP simple
  sh_in "psql -U '$DB_USER' -d postgres -v ON_ERROR_STOP=1 -c \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$DB_NAME' AND pid <> pg_backend_pid();\""
  psql_c postgres "DROP DATABASE IF EXISTS $DB_NAME;"
fi
ok "Base supprimée (le cas échéant)."

info "Création de la base \"$DB_NAME\" (owner $DB_USER)…"
psql_c postgres "CREATE DATABASE $DB_NAME OWNER $DB_USER;"
ok "Base créée."

# --- Restauration ---
info "Restauration depuis $(basename "$BACKUP_FILE") …"

# Déterminer si gzip
case "$TARGET_IN_CONTAINER" in
  *.gz)
    sh_in "gunzip -c '$TARGET_IN_CONTAINER' | pg_restore -U '$DB_USER' -d '$DB_NAME' -v --no-owner --no-privileges --role='$DB_USER' --clean --if-exists"
    ;;
  *.dump|*.backup|*.pgc|*.fc)
    sh_in "pg_restore -U '$DB_USER' -d '$DB_NAME' -v --no-owner --no-privileges --role='$DB_USER' --clean --if-exists '$TARGET_IN_CONTAINER'"
    ;;
  *)
    # Détecter vite fait le format custom
    if sh_in "file -b '$TARGET_IN_CONTAINER' | grep -qi 'PostgreSQL custom database dump'"; then
      sh_in "pg_restore -U '$DB_USER' -d '$DB_NAME' -v --no-owner --no-privileges --role='$DB_USER' --clean --if-exists '$TARGET_IN_CONTAINER'"
    else
      # Tentative via psql (cas d’un .sql)
      sh_in "psql -U '$DB_USER' -d '$DB_NAME' -v ON_ERROR_STOP=1 < '$TARGET_IN_CONTAINER'"
    fi
    ;;
esac
ok "Restauration terminée."

# --- Vérification ---
info "Vérification des tables…"
sh_in "psql -U '$DB_USER' -d '$DB_NAME' -c \"\\dt\""

# --- Relancer services ---
info "Relance des services applicatifs (si présents)…"
set +e
$DC up -d web worker frontend >/dev/null 2>&1
set -e
ok "Terminé. 🚀"
