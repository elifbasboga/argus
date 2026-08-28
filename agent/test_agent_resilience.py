"""
test_agent_resilience.py — Argus dayanıklılık testleri (otomatik).

Gerçek sunucu/sudo gerektirmez: ağ çağrıları VE loglama mock'lanır.
Loglamayı da mock'lamamızın sebebi: agent.py hata durumunda log_event
çağırıp gerçek dosyaya yazıyor. Bu dosya (logs/argus_commands.log)
daha önce `sudo python3 agent.py` ile oluşturulduysa root'a ait olur,
ve testleri sudo'suz çalıştırdığında PermissionError alırsın — bu,
kodun kendisiyle değil, dosya sahipliğiyle ilgili bir sorun. Testleri
disk erişiminden tamamen bağımsız hale getirerek bu kırılganlığı
kalıcı olarak ortadan kaldırıyoruz.

Çalıştırma:
    pip install pytest
    pytest test_agent_resilience.py -v
"""

from unittest.mock import patch, MagicMock

import pytest
import requests

import agent


# ---------------------------------------------------------------------------
# Retry gecikmesi testleri — SABİT aralık, katlanarak artmıyor
# ---------------------------------------------------------------------------
def test_retry_delay_equals_fixed_interval():
    assert agent.calculate_retry_delay(1) == agent.RETRY_INTERVAL


def test_retry_delay_does_not_grow_with_failures():
    # Kaç kez üst üste başarısız olursa olsun, gecikme hep aynı kalmalı
    # (bilinçli tercih: exponential backoff DEĞİL, sabit aralık)
    delays = [agent.calculate_retry_delay(n) for n in (1, 2, 5, 20, 100)]
    assert all(d == agent.RETRY_INTERVAL for d in delays)


# ---------------------------------------------------------------------------
# Komut sonucu gönderim / kuyruk testleri
# (requests.post VE log_event mock'lanıyor -> gerçek ağ/disk erişimi yok)
# ---------------------------------------------------------------------------
def test_send_command_result_success_returns_true():
    with patch("agent.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)
        result = agent.send_command_result({"command": "get_system_info", "status": "ok", "data": {}})
        assert result is True
        mock_post.assert_called_once()


def test_send_command_result_failure_returns_false():
    with patch("agent.requests.post", side_effect=requests.ConnectionError("refused")), \
         patch("agent.log_event") as mock_log:
        result = agent.send_command_result({"command": "get_system_info", "status": "ok", "data": {}})
        assert result is False
        mock_log.assert_called_once()  # başarısızlık loglanmış olmalı, disk'e değil mock'a


def test_flush_pending_results_clears_queue_on_success():
    agent._pending_result_queue.clear()
    agent._pending_result_queue.append({"command": "kill_process", "status": "ok", "data": {}})
    agent._pending_result_queue.append({"command": "get_system_info", "status": "ok", "data": {}})

    with patch("agent.requests.post") as mock_post, patch("agent.log_event"):
        mock_post.return_value = MagicMock(status_code=200)
        agent.flush_pending_results()

    assert agent._pending_result_queue == []
    mock_post.assert_called_once()
    # Toplu gönderim tek istekte, iki sonucu birlikte taşımalı
    _, kwargs = mock_post.call_args
    assert len(kwargs["json"]["results"]) == 2


def test_flush_pending_results_keeps_queue_on_failure():
    agent._pending_result_queue.clear()
    agent._pending_result_queue.append({"command": "kill_process", "status": "ok", "data": {}})

    with patch("agent.requests.post", side_effect=requests.ConnectionError("refused")), \
         patch("agent.log_event"):
        agent.flush_pending_results()

    # Sunucu hâlâ erişilemezse sonuç kuyrukta kalmalı, kaybolmamalı
    assert len(agent._pending_result_queue) == 1


def test_flush_pending_results_noop_when_queue_empty():
    agent._pending_result_queue.clear()
    with patch("agent.requests.post") as mock_post:
        agent.flush_pending_results()
    mock_post.assert_not_called()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
