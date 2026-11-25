"""Console display settings stored in console.db (no browser storage in the page)."""

import unittest

import test_console as console_tests
from astrbot_plugin_super_admin.main import SETTINGS, console_settings
from astrbot_plugin_super_admin.store import Store

DEFAULTS = {
    "auto_images": False,
    "avatars": False,
    "send_key": "ctrl_enter",
    "reply_mode": "confirm",
    "message_actions": "always",
    "image_save": "manual",
    "image_quota": 1024,
}


class SettingsTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = console_tests.ConsoleTests.asyncSetUp
    asyncTearDown = console_tests.ConsoleTests.asyncTearDown
    post = console_tests.ConsoleTests.post
    get = console_tests.ConsoleTests.get

    async def test_defaults_keep_the_previous_behaviour(self):
        self.assertEqual(
            {key: allowed[0] for key, allowed in SETTINGS.items()}, DEFAULTS
        )
        self.assertEqual(await self.get("settings", {}), DEFAULTS)

    async def test_saves_only_sent_keys(self):
        status, body = await self.post("settings", {"avatars": True})
        self.assertEqual(status, 200)
        self.assertEqual(body, {**DEFAULTS, "avatars": True})
        status, body = await self.post(
            "settings", {"send_key": "enter", "auto_images": True}
        )
        self.assertEqual(status, 200)
        expected = {
            **DEFAULTS,
            "avatars": True,
            "send_key": "enter",
            "auto_images": True,
        }
        self.assertEqual(body, expected)
        self.assertEqual(await self.get("settings", {}), expected)

    async def test_rejects_unknown_keys_and_values(self):
        await self.post("settings", {"reply_mode": "auto"})
        for body in (
            {"theme": "dark"},
            {"avatars": 1},
            {"avatars": "true"},
            {"auto_images": None},
            {"send_key": "shift_enter"},
            {"reply_mode": True},
            {"message_actions": "hover", "unknown": True},
            {},
            ["avatars"],
            "avatars",
        ):
            with self.subTest(body=body):
                status, result = await self.post("settings", body)
                self.assertEqual(status, 400)
                self.assertEqual(result["message"], "无效设置")
        # A rejected request saves nothing, not even its valid keys.
        self.assertEqual(
            await self.get("settings", {}), {**DEFAULTS, "reply_mode": "auto"}
        )

    async def test_stale_rows_fall_back_and_values_survive_reopening(self):
        p = self.plugin
        for key, value in (
            ("old_option", "true"),
            ("avatars", "not json"),
            ("auto_images", "1"),
            ("send_key", '"shift_enter"'),
        ):
            p.store.execute("INSERT INTO settings(key,value) VALUES(?,?)", (key, value))
        self.assertEqual(await self.get("settings", {}), DEFAULTS)
        await self.post("settings", {"avatars": True, "message_actions": "hover"})
        # A new connection (as after a restart) reads the saved values.
        path = p.store.db.execute("PRAGMA database_list").fetchone()["file"]
        reopened = Store(path)
        self.addCleanup(reopened.close)
        self.assertEqual(
            console_settings(reopened.rows("SELECT key,value FROM settings")),
            {**DEFAULTS, "avatars": True, "message_actions": "hover"},
        )

    async def test_refused_while_reloading(self):
        self.plugin.terminating = True
        status, body = await self.post("settings", {"avatars": True})
        self.assertEqual(status, 400)
        self.assertIn("重载", body["message"])
        self.plugin.terminating = False
        self.assertEqual(await self.get("settings", {}), DEFAULTS)


if __name__ == "__main__":
    unittest.main()
