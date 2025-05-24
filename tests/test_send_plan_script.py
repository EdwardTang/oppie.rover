import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import send_codex_plan_request


def test_send_codex_plan_request_no_tty(monkeypatch):
    logs = Path('.scratchpad_logs')
    logs.mkdir(exist_ok=True)
    req = logs / 'test_plan_request.md'
    req.write_text('Test plan request', encoding='utf-8')

    monkeypatch.setattr('oppie_xyz.utils.cli_utils.run_codex_safe', lambda **kwargs: 'ok')
    monkeypatch.setattr(send_codex_plan_request, 'run_codex_safe', lambda **kwargs: 'ok')
    monkeypatch.setattr(send_codex_plan_request, 'SCRATCHPAD_LOGS', logs)

    sys.argv = [
        'send_codex_plan_request.py',
        '--provider', 'OAI',
        '--model', 'o3',
        '--project-doc', 'scratchpad.md',
        '--no-interactive',
    ]
    send_codex_plan_request.main()
