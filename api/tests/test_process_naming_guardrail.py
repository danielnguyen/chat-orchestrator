from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_process_naming.py"


def load_scanner():
    spec = importlib.util.spec_from_file_location("check_process_naming", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def diff_for(path: str, *lines: str, new_file: bool = False) -> str:
    header = [f"diff --git a/{path} b/{path}"]
    if new_file:
        header.append("new file mode 100644")
        header.append("--- /dev/null")
    else:
        header.append(f"--- a/{path}")
    header.append(f"+++ b/{path}")
    header.append(f"@@ -0,0 +1,{len(lines)} @@")
    return "\n".join([*header, *lines, ""])


def test_added_forbidden_term_fails_anywhere() -> None:
    scanner = load_scanner()
    bad = "wa" + "ve3b"
    findings = scanner.scan_diff_text(diff_for("README.md", f"+{bad} result boundary"))
    assert [(finding.path, finding.token, finding.line) for finding in findings] == [
        ("README.md", bad, 1)
    ]


def test_added_new_file_path_fails() -> None:
    scanner = load_scanner()
    bad = "clus" + "ter20"
    path = f"docs/{bad}.md"
    findings = scanner.scan_diff_text(diff_for(path, "+clean text", new_file=True))
    assert [(finding.path, finding.token, finding.line) for finding in findings] == [
        (path, bad, None)
    ]


def test_existing_dirty_file_path_is_not_reflagged_for_clean_edit() -> None:
    scanner = load_scanner()
    bad = "wa" + "ve3b"
    path = f"scripts/{bad}_smoke.sh"
    findings = scanner.scan_diff_text(diff_for(path, "+echo clean"))
    assert findings == []


def test_deleted_terms_are_ignored() -> None:
    scanner = load_scanner()
    bad = "pha" + "se4"
    findings = scanner.scan_diff_text(diff_for("README.md", f"-{bad} old wording"))
    assert findings == []


_AUTHORIZATION_TERM = "pha" + "se"
_AUTHORIZATION_EXAMPLES = [
    f'authorization_{_AUTHORIZATION_TERM}: Literal["exposure"]',
    f'{_AUTHORIZATION_TERM}: Literal["exposure"]',
    f'"{_AUTHORIZATION_TERM}": "exposure", "confirmation_state": "not_required",',
    '"active_persona_id": "technical_architect", '
    f'"authorization_{_AUTHORIZATION_TERM}": "exposure",',
    f'"{_AUTHORIZATION_TERM}": "exposure", "allowed": True, "decision_code": "allowed",',
    f'("authorization_{_AUTHORIZATION_TERM}", "dispatch"), ("argument_digest", "capargs_digest"),',
    '("result.decision_code", "confirmation_accepted"), '
    f'("result.{_AUTHORIZATION_TERM}", "dispatch"),',
]


@pytest.mark.parametrize("text", _AUTHORIZATION_EXAMPLES)
def test_exact_authorization_contract_examples_are_accepted(text) -> None:
    scanner = load_scanner()
    assert scanner.scan_diff_text(diff_for("api/clients/runtime.py", f"+{text}")) == []


@pytest.mark.parametrize("stage", ["exposure", "selection", "dispatch"])
@pytest.mark.parametrize("quote", ["'", '"'])
@pytest.mark.parametrize("shape", [
    "annotation", "mapping", "candidate-mutation", "result-mutation",
])
def test_bounded_authorization_field_syntax_is_accepted(stage, quote, shape) -> None:
    scanner = load_scanner()
    term = _AUTHORIZATION_TERM
    quoted_stage = f"{quote}{stage}{quote}"
    text = {
        "annotation": f"authorization_{term}: Literal[ {quoted_stage} ]",
        "mapping": f"{quote}{term}{quote}: {quoted_stage}",
        "candidate-mutation": f"( {quote}authorization_{term}{quote}, {quoted_stage} )",
        "result-mutation": f"( {quote}result.{term}{quote}, {quoted_stage} )",
    }[shape]
    assert scanner.detect_in_text("api/clients/runtime.py", text, 1) == []


@pytest.mark.parametrize("text", [
    _AUTHORIZATION_TERM,
    f"project_{_AUTHORIZATION_TERM}: str",
    f"project_{_AUTHORIZATION_TERM}: Literal['exposure']",
    f"obj.authorization_{_AUTHORIZATION_TERM}: Literal['exposure']",
    f"authorization_{_AUTHORIZATION_TERM}: str",
    f"authorization_{_AUTHORIZATION_TERM}: Literal['unknown']",
    f"'{_AUTHORIZATION_TERM}': 'planning'",
    f"'{_AUTHORIZATION_TERM}': 'exposure-extra'",
    f"'project_{_AUTHORIZATION_TERM}': 'exposure'",
    f"('project.{_AUTHORIZATION_TERM}', 'dispatch')",
    f"('result.{_AUTHORIZATION_TERM}', 'unknown')",
    f"('result.{_AUTHORIZATION_TERM}', 'dispatch', 'extra')",
    f"'{_AUTHORIZATION_TERM}' == 'exposure'",
    f'''"{_AUTHORIZATION_TERM}': 'exposure' ''',
])
def test_unrecognized_fields_and_values_are_not_exempt(text) -> None:
    scanner = load_scanner()
    findings = scanner.detect_in_text("api/clients/runtime.py", text, 1)
    assert any(item.token == _AUTHORIZATION_TERM for item in findings)


@pytest.mark.parametrize("text", _AUTHORIZATION_EXAMPLES)
@pytest.mark.parametrize("marker", [
    "pha" + "se6", "pha" + "se", "clus" + "ter1", "wa" + "ve3",
    "sub" + "wa" + "ve", "R" + "42", "CO" + "-24", "PR" + "204",
    "PR" + " #204", "implementation " + "pa" + "ss",
])
@pytest.mark.parametrize("position", ["before", "after"])
def test_authorization_exemption_preserves_other_markers_on_the_same_line(
    text, marker, position,
) -> None:
    scanner = load_scanner()
    mixed = f"{marker}; {text}" if position == "before" else f"{text}; {marker}"
    findings = scanner.scan_diff_text(diff_for("api/clients/runtime.py", f"+{mixed}"))
    expected = scanner.detect_in_text("api/clients/runtime.py", marker, 1)
    assert expected
    assert [(item.token, item.pattern) for item in findings] == [
        (item.token, item.pattern) for item in expected
    ]


def test_multiple_authorization_fields_do_not_hide_an_unrelated_term() -> None:
    scanner = load_scanner()
    term = _AUTHORIZATION_TERM
    text = f'''"{term}": "exposure", "authorization_{term}": "selection", "project_{term}": 6'''
    findings = scanner.detect_in_text("api/clients/runtime.py", text, 1)
    assert [(item.token, item.pattern) for item in findings] == [(term, "delivery-term")]
