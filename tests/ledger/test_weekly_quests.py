import hashlib
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError
from django.test import RequestFactory

from ledger.dashboard import config
from ledger.dashboard.models import (
    QuestOwnerIdentity,
    QuestQualifiedVisit,
    QuestShareCampaign,
    RewardChestGrant,
)
from ledger.dashboard.reward_chests import open_reward_chest
from ledger.dashboard.weekly_quests import (
    ATTRIBUTION_COOKIE_NAME,
    VISITOR_COOKIE_NAME,
    _cycle_for_datetime,
    _definition_from_config,
    _fingerprint_hash,
    _network_hash,
    _quest_progress,
    _visitor_hash,
    _weekly_row,
    build_share_redirect_response,
    build_weekly_quest_status,
    get_weekly_definitions,
    prepare_weekly_quest_reward,
    record_navigation,
)
from tests.ledger.base import BaseLedgerTestCase


class TestWeeklyQuestHardening(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.factory = RequestFactory()

    @staticmethod
    def _digest(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _visitor_token(self, value: str) -> str:
        return self._digest(f"visitor:{value}")

    def _fingerprint(self, value: str) -> str:
        return self._digest(f"fingerprint:{value}")

    def _platform(self) -> str:
        return next(iter(config.QUEST_BOARD_SOCIAL_HOSTS))

    def _unfurl_platform(self) -> tuple[str, str]:
        for platform, fragments in config.QUEST_BOARD_UNFURL_USER_AGENTS.items():
            if fragments:
                return platform, str(fragments[0])
        self.fail("Weekly quest config needs at least one unfurl UA for this test")

    def _chest_key(self) -> str:
        return next(iter(config.get_reward_chest_definitions()))

    def _video_quest_config(self, *, key: str, target: int = 1, platform=None):
        platform = platform or self._platform()
        return {
            key: {
                "enabled": True,
                "title": f"Quest {key}",
                "description": "Tracked link test quest",
                "condition": "video_share",
                "platform": platform,
                "icon_material": "share",
                "action_label": "Choose Video",
                "action_url": "/latest",
                "target": target,
                "progress_pending_text": "Waiting {current} / {target}",
                "progress_complete_text": "Verified",
                "reward": {
                    "kind": "chest",
                    "chest": self._chest_key(),
                },
            },
        }

    def _campaign(
        self,
        *,
        quest_key: str,
        campaign_suffix: str,
        platform=None,
        target_path=None,
        owner=None,
    ) -> QuestShareCampaign:
        cycle = _cycle_for_datetime()
        owner = owner or self.u1
        return QuestShareCampaign.objects.create(
            campaign_key=self._digest(
                f"{owner.pk}:{cycle.key}:{quest_key}:{campaign_suffix}"
            ),
            owner=owner,
            cycle_key=cycle.key,
            quest_key=quest_key,
            campaign_type=QuestShareCampaign.TYPE_VIDEO,
            expected_platform=platform or self._platform(),
            target_path=target_path or f"/tracked/{campaign_suffix}/",
        )

    def _redirect(
        self,
        *,
        campaign,
        visitor_token,
        ip="203.0.113.10",
        user_agent="Mozilla/5.0",
        referer=None,
    ):
        headers = {
            "REMOTE_ADDR": ip,
            "HTTP_USER_AGENT": user_agent,
        }
        if referer is not None:
            headers["HTTP_REFERER"] = referer
        request = self.factory.get("/quest-redirect/", **headers)
        request.user = AnonymousUser()
        request.COOKIES[VISITOR_COOKIE_NAME] = visitor_token
        return build_share_redirect_response(
            request=request,
            public_id=campaign.public_id,
        )

    def _qualify_video(
        self,
        *,
        campaign,
        visitor_token,
        fingerprint,
        ip="203.0.113.10",
        referer=None,
    ):
        response = self._redirect(
            campaign=campaign,
            visitor_token=visitor_token,
            ip=ip,
            referer=referer,
        )
        self.assertIn(ATTRIBUTION_COOKIE_NAME, response.cookies)

        request = self.factory.post(
            "/api/weekly-quests/navigation",
            REMOTE_ADDR=ip,
            HTTP_USER_AGENT="Mozilla/5.0",
        )
        request.user = AnonymousUser()
        request.COOKIES[VISITOR_COOKIE_NAME] = visitor_token
        request.COOKIES[ATTRIBUTION_COOKIE_NAME] = response.cookies[
            ATTRIBUTION_COOKIE_NAME
        ].value
        return record_navigation(
            request=request,
            fingerprint=fingerprint,
            page=campaign.target_path,
        )

    def _controlled_weekly_config(self, *, target=1):
        quest_key = "controlled_video_quest"
        return quest_key, self._video_quest_config(
            key=quest_key,
            target=target,
        )

    def test_weekly_reads_do_not_create_reward_grants(self):
        quest_key, definitions = self._controlled_weekly_config()
        cycle = _cycle_for_datetime()
        fixed_now = cycle.starts_at + timedelta(hours=1)
        completed_progress = {
            "current": 1,
            "target": 1,
            "complete": True,
            "progress_percent": 100,
            "progress_text": "Verified",
        }

        with (
            patch.object(config, "QUEST_BOARD_ENABLED", True),
            patch.object(config, "QUEST_BOARD_WEEKLY_ENABLED", True),
            patch.object(config, "QUEST_BOARD_SLOT_COUNT", 1),
            patch.object(config, "QUEST_BOARD_WEEKLY_SELECTION_SALT", "test-salt"),
            patch.object(config, "QUEST_BOARD_WEEKLY_QUESTS", definitions),
            patch(
                "ledger.dashboard.weekly_quests._quest_progress",
                return_value=completed_progress,
            ),
            patch(
                "ledger.dashboard.weekly_quests.build_starter_quest_board_context",
                return_value={"slots": ()},
            ),
            patch(
                "ledger.dashboard.weekly_quests.timezone.now",
                return_value=fixed_now,
            ),
        ):
            definition = get_weekly_definitions(
                user=self.u1,
                cycle=cycle,
            )[0]
            before = RewardChestGrant.objects.filter(
                source_type="weekly_quest"
            ).count()

            row = _weekly_row(
                user=self.u1,
                cycle=cycle,
                definition=definition,
            )
            build_weekly_quest_status(user=self.u1)

            self.assertEqual(
                RewardChestGrant.objects.filter(
                    source_type="weekly_quest"
                ).count(),
                before,
            )
            self.assertEqual(row["key"], quest_key)
            self.assertTrue(row["complete"])
            self.assertTrue(row["can_claim"])

    def test_weekly_grant_is_created_on_prepare_and_expires_at_reset(self):
        quest_key, definitions = self._controlled_weekly_config()
        cycle = _cycle_for_datetime()
        fixed_now = cycle.starts_at + timedelta(hours=1)
        balance_before = int(self.w1.balance)

        with (
            patch.object(config, "QUEST_BOARD_ENABLED", True),
            patch.object(config, "QUEST_BOARD_WEEKLY_ENABLED", True),
            patch.object(config, "QUEST_BOARD_SLOT_COUNT", 1),
            patch.object(config, "QUEST_BOARD_WEEKLY_SELECTION_SALT", "test-salt"),
            patch.object(config, "QUEST_BOARD_WEEKLY_QUESTS", definitions),
            patch(
                "ledger.dashboard.weekly_quests._quest_progress",
                return_value={
                    "current": 1,
                    "target": 1,
                    "complete": True,
                    "progress_percent": 100,
                    "progress_text": "Verified",
                },
            ),
            patch(
                "ledger.dashboard.weekly_quests.timezone.now",
                return_value=fixed_now,
            ),
            patch(
                "ledger.dashboard.reward_chests.timezone.now",
                return_value=fixed_now,
            ),
        ):
            prepared = prepare_weekly_quest_reward(
                user=self.u1,
                cycle_key=cycle.key,
                quest_key=quest_key,
            )

        grant = prepared["grant"]
        self.assertEqual(grant.status, RewardChestGrant.STATUS_PENDING)
        self.assertEqual(grant.expires_at, cycle.ends_at)
        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, balance_before)

        with self.assertRaises(ValidationError):
            open_reward_chest(
                user=self.u1,
                grant=grant,
                at=cycle.ends_at,
            )

    def test_same_identity_counts_once_per_owner_quest_not_once_per_week(self):
        shared_visitor = self._visitor_token("shared")
        shared_fingerprint = self._fingerprint("shared")
        first = self._campaign(
            quest_key="quest-a",
            campaign_suffix="a-1",
        )
        same_quest_second_campaign = self._campaign(
            quest_key="quest-a",
            campaign_suffix="a-2",
        )
        other_quest = self._campaign(
            quest_key="quest-b",
            campaign_suffix="b-1",
        )

        with (
            patch.object(config, "QUEST_BOARD_ENABLED", True),
            patch.object(config, "QUEST_BOARD_WEEKLY_ENABLED", True),
        ):
            first_result = self._qualify_video(
                campaign=first,
                visitor_token=shared_visitor,
                fingerprint=shared_fingerprint,
            )
            duplicate_result = self._qualify_video(
                campaign=same_quest_second_campaign,
                visitor_token=shared_visitor,
                fingerprint=shared_fingerprint,
            )
            other_quest_result = self._qualify_video(
                campaign=other_quest,
                visitor_token=shared_visitor,
                fingerprint=shared_fingerprint,
            )

        self.assertTrue(first_result["qualified"])
        self.assertFalse(duplicate_result["qualified"])
        self.assertTrue(other_quest_result["qualified"])
        self.assertEqual(
            QuestQualifiedVisit.objects.filter(
                campaign__owner=self.u1,
                campaign__quest_key="quest-a",
            ).count(),
            1,
        )
        self.assertEqual(
            QuestQualifiedVisit.objects.filter(
                campaign__owner=self.u1,
                campaign__quest_key="quest-b",
            ).count(),
            1,
        )


    def test_same_identity_can_count_for_different_owners(self):
        visitor = self._visitor_token("cross-owner")
        fingerprint = self._fingerprint("cross-owner")
        first = self._campaign(
            quest_key="shared-quest",
            campaign_suffix="owner-one",
            owner=self.u1,
        )
        second = self._campaign(
            quest_key="shared-quest",
            campaign_suffix="owner-two",
            owner=self.u2,
        )

        with (
            patch.object(config, "QUEST_BOARD_ENABLED", True),
            patch.object(config, "QUEST_BOARD_WEEKLY_ENABLED", True),
        ):
            first_result = self._qualify_video(
                campaign=first,
                visitor_token=visitor,
                fingerprint=fingerprint,
            )
            second_result = self._qualify_video(
                campaign=second,
                visitor_token=visitor,
                fingerprint=fingerprint,
            )

        self.assertTrue(first_result["qualified"])
        self.assertTrue(second_result["qualified"])
        self.assertEqual(
            QuestQualifiedVisit.objects.filter(
                visitor_hash=_visitor_hash(visitor),
            ).count(),
            2,
        )

    def test_shared_ip_is_not_a_unique_visitor_or_self_referral_key(self):
        ip = "198.51.100.20"
        campaign = self._campaign(
            quest_key="shared-ip-quest",
            campaign_suffix="shared-ip",
        )

        owner_visitor = self._visitor_token("owner")
        owner_fingerprint = self._fingerprint("owner")
        owner_request = self.factory.get("/", REMOTE_ADDR=ip)
        QuestOwnerIdentity.objects.create(
            user=self.u1,
            cycle_key=campaign.cycle_key,
            network_hash=_network_hash(owner_request),
            fingerprint_hash=_fingerprint_hash(owner_fingerprint),
            visitor_hash=_visitor_hash(owner_visitor),
        )

        with (
            patch.object(config, "QUEST_BOARD_ENABLED", True),
            patch.object(config, "QUEST_BOARD_WEEKLY_ENABLED", True),
        ):
            first = self._qualify_video(
                campaign=campaign,
                visitor_token=self._visitor_token("visitor-one"),
                fingerprint=self._fingerprint("visitor-one"),
                ip=ip,
            )
            second = self._qualify_video(
                campaign=campaign,
                visitor_token=self._visitor_token("visitor-two"),
                fingerprint=self._fingerprint("visitor-two"),
                ip=ip,
            )

        self.assertTrue(first["qualified"])
        self.assertTrue(second["qualified"])
        self.assertEqual(
            QuestQualifiedVisit.objects.filter(campaign=campaign).count(),
            2,
        )

    def test_owner_visitor_or_fingerprint_is_still_rejected(self):
        campaign = self._campaign(
            quest_key="self-referral-quest",
            campaign_suffix="self-referral",
        )
        owner_visitor = self._visitor_token("owner-identity")
        owner_fingerprint = self._fingerprint("owner-identity")
        owner_request = self.factory.get("/", REMOTE_ADDR="203.0.113.30")
        QuestOwnerIdentity.objects.create(
            user=self.u1,
            cycle_key=campaign.cycle_key,
            network_hash=_network_hash(owner_request),
            fingerprint_hash=_fingerprint_hash(owner_fingerprint),
            visitor_hash=_visitor_hash(owner_visitor),
        )

        with (
            patch.object(config, "QUEST_BOARD_ENABLED", True),
            patch.object(config, "QUEST_BOARD_WEEKLY_ENABLED", True),
        ):
            result = self._qualify_video(
                campaign=campaign,
                visitor_token=owner_visitor,
                fingerprint=owner_fingerprint,
                ip="198.51.100.99",
            )

        self.assertFalse(result["qualified"])
        self.assertFalse(
            QuestQualifiedVisit.objects.filter(campaign=campaign).exists()
        )

    def test_tracked_video_click_needs_no_referer_and_unfurl_does_not_qualify(self):
        platform, unfurl_fragment = self._unfurl_platform()
        campaign = self._campaign(
            quest_key="tracked-social-quest",
            campaign_suffix="tracked-social",
            platform=platform,
        )
        visitor = self._visitor_token("real-visitor")

        with (
            patch.object(config, "QUEST_BOARD_ENABLED", True),
            patch.object(config, "QUEST_BOARD_WEEKLY_ENABLED", True),
        ):
            unfurl_response = self._redirect(
                campaign=campaign,
                visitor_token=self._visitor_token("unfurl"),
                user_agent=f"{unfurl_fragment} test",
            )
            self.assertNotIn(
                ATTRIBUTION_COOKIE_NAME,
                unfurl_response.cookies,
            )
            self.assertFalse(
                QuestQualifiedVisit.objects.filter(campaign=campaign).exists()
            )

            real_result = self._qualify_video(
                campaign=campaign,
                visitor_token=visitor,
                fingerprint=self._fingerprint("real-visitor"),
                referer=None,
            )

        self.assertTrue(real_result["qualified"])
        visit = QuestQualifiedVisit.objects.get(campaign=campaign)
        self.assertEqual(visit.referer_host, "")
        self.assertFalse(
            RewardChestGrant.objects.filter(source_type="weekly_quest").exists()
        )

    def test_video_share_target_is_configurable(self):
        target = 3
        quest_key = "configurable-target"
        definitions = self._video_quest_config(
            key=quest_key,
            target=target,
        )
        cycle = _cycle_for_datetime()

        with patch.object(config, "QUEST_BOARD_WEEKLY_QUESTS", definitions):
            definition = _definition_from_config(quest_key)

        campaign = self._campaign(
            quest_key=quest_key,
            campaign_suffix="target",
            platform=definition.platform,
        )
        for index in range(target - 1):
            QuestQualifiedVisit.objects.create(
                campaign=campaign,
                cycle_key=cycle.key,
                visitor_hash=self._digest(f"target-visitor:{index}"),
                network_hash=self._digest(f"target-network:{index}"),
                fingerprint_hash=self._digest(f"target-fingerprint:{index}"),
                landing_page=campaign.target_path,
                qualification_type=QuestQualifiedVisit.TYPE_VIDEO_PLATFORM,
            )

        progress = _quest_progress(
            user=self.u1,
            cycle=cycle,
            definition=definition,
        )
        self.assertEqual(progress["target"], target)
        self.assertEqual(progress["current"], target - 1)
        self.assertFalse(progress["complete"])
        self.assertEqual(
            progress["progress_percent"],
            (target - 1) * 100 // target,
        )

        final_index = target - 1
        QuestQualifiedVisit.objects.create(
            campaign=campaign,
            cycle_key=cycle.key,
            visitor_hash=self._digest(f"target-visitor:{final_index}"),
            network_hash=self._digest(f"target-network:{final_index}"),
            fingerprint_hash=self._digest(f"target-fingerprint:{final_index}"),
            landing_page=campaign.target_path,
            qualification_type=QuestQualifiedVisit.TYPE_VIDEO_PLATFORM,
        )
        progress = _quest_progress(
            user=self.u1,
            cycle=cycle,
            definition=definition,
        )
        self.assertEqual(progress["current"], target)
        self.assertTrue(progress["complete"])
        self.assertEqual(progress["progress_percent"], 100)
