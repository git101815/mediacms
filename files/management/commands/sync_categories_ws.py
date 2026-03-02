from django.core.management.base import BaseCommand

from django.db import transaction

from collections import defaultdict

import re


from files.models import Media, Category  # app_label confirmé



def norm(s: str) -> str:

    """Même règle que sync_celebrities_ws.py : lower + suppression des espaces."""

    return re.sub(r"\s+", "", (s or "")).lower()



class Command(BaseCommand):

    help = (

        "Associe automatiquement des catégories aux médias via les tags "

        "(match exact après lower() et suppression des espaces). "

        "Par défaut: ne traite que les médias sans catégorie."

    )


    def add_arguments(self, parser):

        parser.add_argument("--dry-run", action="store_true",

                            help="N'écrit rien en base (aperçu).")

        parser.add_argument("--limit", type=int,

                            help="Traite au plus N médias.")

        parser.add_argument("--even-if-existing", action="store_true",

                            help="Traite aussi les médias ayant déjà des catégories.")

        parser.add_argument("--verbose", action="store_true",

                            help="Logs détaillés.")


    def handle(self, *args, **opts):

        dry = opts["dry_run"]

        limit = opts.get("limit")

        even_if_existing = opts["even_if_existing"]

        verbose = opts["verbose"]


        # Index catégories : clé normalisée -> [id, id, ...]

        by_key = defaultdict(list)

        for cid, title in Category.objects.values_list("id", "title"):

            k = norm(title)

            if k:

                by_key[k].append(cid)


        # Collisions : même clé pour plusieurs catégories → ignorées par sécurité

        collisions = {k for k, ids in by_key.items() if len(ids) > 1}

        if collisions:

            self.stdout.write(self.style.WARNING(

                f"⚠️ {len(collisions)} clés en collision (ignorées)."

            ))


        qs = Media.objects.all().prefetch_related("tags", "category")


        # Par défaut on ne traite que les médias sans catégorie

        if not even_if_existing:

            qs = qs.filter(category__isnull=True)


        if limit:

            qs = qs[:limit]


        updated = 0

        links_added = 0


        with transaction.atomic():

            for m in qs.iterator(chunk_size=500):

                current_ids = set(m.category.values_list("id", flat=True))

                to_add = set()


                for t_title in m.tags.values_list("title", flat=True):

                    key = norm(t_title)

                    if not key or key in collisions:

                        continue

                    for cid in by_key.get(key, []):

                        if cid not in current_ids:

                            to_add.add(cid)


                if to_add:

                    if not dry:

                        m.category.add(*to_add)

                    updated += 1

                    links_added += len(to_add)

                    if verbose or dry:

                        self.stdout.write(

                            f"Media {m.pk}: +{len(to_add)} catégorie(s){' [DRY]' if dry else ''}"

                        )


        self.stdout.write(self.style.SUCCESS(

            f"✅ Médias mis à jour: {updated}, liens ajoutés: {links_added}"

            + (" (dry-run)" if dry else "")

        ))
