"""Implement a test by a simulating an OCPP 2.0.1 chargepoint."""

import asyncio
import copy
from datetime import datetime, UTC

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ocpp.const import CONF_CPIDS, CONF_CPID, CONF_PORT
from custom_components.ocpp.const import (
    DOMAIN as OCPP_DOMAIN,
)
from custom_components.ocpp.enums import (
    HAChargerServices as csvcs,
)
import ocpp
from ocpp.routing import on
import ocpp.exceptions
from ocpp.v201 import ChargePoint as cpclass, call, call_result
from ocpp.v201.enums import (
    Action,
    BootReasonEnumType,
    ChangeAvailabilityStatusEnumType,
    ChargingStateEnumType,
    ConnectorStatusEnumType,
    DataEnumType,
    GenericDeviceModelStatusEnumType,
    MutabilityEnumType,
    OperationalStatusEnumType,
    ReadingContextEnumType,
    RegistrationStatusEnumType,
    ReportBaseEnumType,
    SetVariableStatusEnumType,
    TransactionEventEnumType,
    TriggerMessageStatusEnumType,
    TriggerReasonEnumType,
    UpdateFirmwareStatusEnumType,
)
from ocpp.v201.datatypes import (
    ComponentType,
    EVSEType,
    VariableType,
    VariableAttributeType,
    VariableCharacteristicsType,
    ReportDataType,
    SetVariableResultType,
)

from .const import MOCK_CONFIG_DATA, MOCK_CONFIG_CP_APPEND

from .charge_point_test import (
    create_configuration,
    run_charge_point_test,
    remove_configuration,
    wait_ready,
)


class MultiConnectorChargePoint(cpclass):
    """Minimal OCPP 2.0.1 client that reports 2 EVSE (1: two connectors, 2: one)."""

    def __init__(self, cp_id, ws):
        """Initialize."""
        super().__init__(cp_id, ws)
        self.inventory_done = asyncio.Event()
        self.last_start_evse_id = None
        self._tasks: set[asyncio.Task] = set()

    @on(Action.get_base_report)
    async def on_get_base_report(self, request_id: int, report_base: str, **kwargs):
        """Get base report."""
        assert report_base in (ReportBaseEnumType.full_inventory, "FullInventory")
        task = asyncio.create_task(self._send_full_inventory(request_id))
        self._tasks.add(task)
        return call_result.GetBaseReport(
            GenericDeviceModelStatusEnumType.accepted.value
        )

    @on(Action.trigger_message)
    async def on_trigger_message(self, requested_message: str, **kwargs):
        """Handle trigger message."""
        self.last_trigger = (requested_message, kwargs.get("evse"))
        return call_result.TriggerMessage(TriggerMessageStatusEnumType.accepted.value)

    @on(Action.update_firmware)
    async def on_update_firmware(self, request_id: int, firmware: dict, **kwargs):
        """Handle update firmware."""
        return call_result.UpdateFirmware(UpdateFirmwareStatusEnumType.rejected.value)

    @on(Action.set_variables)
    async def on_set_variables(self, set_variable_data: list[dict], **kwargs):
        """Handle SetVariables."""
        results: list[SetVariableResultType] = []
        for item in set_variable_data:
            comp = item.get("component", {})
            var = item.get("variable", {})
            results.append(
                SetVariableResultType(
                    SetVariableStatusEnumType.accepted,
                    ComponentType(
                        comp.get("name"),
                        instance=comp.get("instance"),
                        evse=comp.get("evse"),
                    ),
                    VariableType(
                        var.get("name"),
                        instance=var.get("instance"),
                    ),
                )
            )
        return call_result.SetVariables(results)

    async def _send_full_inventory(self, request_id: int):
        ts = datetime.now(UTC).isoformat()
        report_data = [
            # EVSE 1
            ReportDataType(
                ComponentType("EVSE", evse=EVSEType(1)),
                VariableType("Status"),
                [VariableAttributeType(value="OK")],
            ),
            # EVSE 2
            ReportDataType(
                ComponentType("EVSE", evse=EVSEType(2)),
                VariableType("Status"),
                [VariableAttributeType(value="OK")],
            ),
            # Connector(1,1)
            ReportDataType(
                ComponentType("Connector", evse=EVSEType(1, connector_id=1)),
                VariableType("Enabled"),
                [
                    VariableAttributeType(
                        value="true", mutability=MutabilityEnumType.read_only
                    )
                ],
            ),
            # Connector(1,2)
            ReportDataType(
                ComponentType("Connector", evse=EVSEType(1, connector_id=2)),
                VariableType("Enabled"),
                [
                    VariableAttributeType(
                        value="true", mutability=MutabilityEnumType.read_only
                    )
                ],
            ),
            # Connector(2,1)
            ReportDataType(
                ComponentType("Connector", evse=EVSEType(2, connector_id=1)),
                VariableType("Enabled"),
                [
                    VariableAttributeType(
                        value="true", mutability=MutabilityEnumType.read_only
                    )
                ],
            ),
            # SmartChargingCtrlr.Available
            ReportDataType(
                ComponentType("SmartChargingCtrlr"),
                VariableType("Available"),
                [
                    VariableAttributeType(
                        value="true", mutability=MutabilityEnumType.read_only
                    )
                ],
            ),
            # SampledDataCtrlr.TxUpdatedMeasurands
            ReportDataType(
                ComponentType("SampledDataCtrlr"),
                VariableType("TxUpdatedMeasurands"),
                [VariableAttributeType(value="", persistent=True)],
                VariableCharacteristicsType(
                    DataEnumType.member_list,
                    False,
                    values_list="Energy.Active.Import.Register,Current.Import,Voltage",
                ),
            ),
        ]

        req = call.NotifyReport(
            request_id=request_id,
            generated_at=ts,
            seq_no=0,
            report_data=report_data,
            tbc=False,
        )
        await self.call(req)
        self.inventory_done.set()

    @on(Action.change_availability)
    async def on_change_availability(self, operational_status: str, **kwargs):
        """Handle change availability."""
        self.operative = operational_status == OperationalStatusEnumType.operative.value
        return call_result.ChangeAvailability(
            ChangeAvailabilityStatusEnumType.accepted.value
        )

    @on(Action.request_start_transaction)
    async def on_request_start_transaction(
        self, evse_id: int, id_token: dict, **kwargs
    ):
        """Handle request for start transaction."""
        self.last_start_evse_id = evse_id
        return call_result.RequestStartTransaction(status="Accepted")

    @on(Action.request_stop_transaction)
    async def on_request_stop_transaction(self, transaction_id: str, **kwargs):
        """Handle request for stop transaction."""
        return call_result.RequestStopTransaction(status="Accepted")

    async def send_status(
        self, evse_id: int, connector_id: int, status: ConnectorStatusEnumType
    ):
        """Send status."""
        await self.call(
            call.StatusNotification(
                timestamp=datetime.now(UTC).isoformat(),
                connector_status=status.value,
                evse_id=evse_id,
                connector_id=connector_id,
            )
        )

    async def send_tx_started_eair_wh(
        self, evse_id: int, connector_id: int, tx_id: str, eair_wh: int
    ):
        """Send EAIR on transaction started."""
        await self.call(
            call.TransactionEvent(
                event_type=TransactionEventEnumType.started,
                timestamp=datetime.now(UTC).isoformat(),
                trigger_reason=TriggerReasonEnumType.authorized,
                seq_no=0,
                transaction_info={
                    "transaction_id": tx_id,
                    "charging_state": ChargingStateEnumType.charging,
                },
                evse={"id": evse_id, "connector_id": connector_id},
                meter_value=[
                    {
                        "timestamp": datetime.now(UTC).isoformat(),
                        "sampled_value": [
                            {
                                "measurand": "Energy.Active.Import.Register",
                                "value": eair_wh,
                                "unit_of_measure": {"unit": "Wh"},
                            }
                        ],
                    }
                ],
            )
        )

    async def send_tx_updated_eair_wh(
        self, evse_id: int, connector_id: int, tx_id: str, eair_wh: int
    ):
        """Send EAIR on transaction updated."""
        await self.call(
            call.TransactionEvent(
                event_type=TransactionEventEnumType.updated,
                timestamp=datetime.now(UTC).isoformat(),
                trigger_reason=TriggerReasonEnumType.meter_value_periodic,
                seq_no=1,
                transaction_info={
                    "transaction_id": tx_id,
                    "charging_state": ChargingStateEnumType.charging,
                },
                evse={"id": evse_id, "connector_id": connector_id},
                meter_value=[
                    {
                        "timestamp": datetime.now(UTC).isoformat(),
                        "sampled_value": [
                            {
                                "measurand": "Energy.Active.Import.Register",
                                "value": eair_wh,
                                "unit_of_measure": {"unit": "Wh"},
                            }
                        ],
                    }
                ],
            )
        )


@pytest.mark.timeout(150)
async def test_v201_multi_connectors_per_evse(hass, socket_enabled):
    """Test multi connector per EVSE functionality."""
    cp_id = "CP_v201_multi"

    config_data = copy.deepcopy(MOCK_CONFIG_DATA)
    config_data[CONF_CPIDS].append({cp_id: copy.deepcopy(MOCK_CONFIG_CP_APPEND)})
    config_data[CONF_CPIDS][-1][cp_id][CONF_CPID] = "test_v201_cpid"

    config_entry = MockConfigEntry(
        domain=OCPP_DOMAIN,
        data=config_data,
        entry_id="test_v201_multi",
        title="test_v201_multi",
        version=2,
        minor_version=0,
    )

    cs = await create_configuration(hass, config_entry)
    ocpp.messages.ASYNC_VALIDATION = False

    async def _scenario(hass, cs, cp: MultiConnectorChargePoint):
        boot = await cp.call(
            call.BootNotification(
                {
                    "serial_number": "SERIAL",
                    "model": "MODEL",
                    "vendor_name": "VENDOR",
                    "firmware_version": "VERSION",
                },
                BootReasonEnumType.power_up.value,
            )
        )
        assert boot.status == RegistrationStatusEnumType.accepted.value

        await wait_ready(cs.charge_points["CP_v201_multi"])

        await asyncio.wait_for(cp.inventory_done.wait(), timeout=5)

        cp_srv = cs.charge_points["CP_v201_multi"]
        for _ in range(50):
            if getattr(cp_srv, "num_connectors", 0) == 3:
                break
            await asyncio.sleep(0.1)
        assert cp_srv.num_connectors == 3

        cpid = cp_srv.settings.cpid

        await cp.send_status(1, 1, ConnectorStatusEnumType.available)
        await cp.send_status(1, 2, ConnectorStatusEnumType.occupied)
        await cp.send_status(2, 1, ConnectorStatusEnumType.unavailable)
        await asyncio.sleep(0.05)

        assert cs.get_metric(cpid, "Status.Connector", connector_id=1) == "Available"
        # Occupied has no 1.6 equivalent of its own and maps to Preparing.
        assert cs.get_metric(cpid, "Status.Connector", connector_id=2) == "Preparing"
        assert cs.get_metric(cpid, "Status.Connector", connector_id=3) == "Unavailable"

        await cp.send_tx_started_eair_wh(1, 2, "TX-1", 10_000)
        await cp.send_tx_updated_eair_wh(1, 2, "TX-1", 10_500)
        await asyncio.sleep(0.05)

        assert cs.get_metric(
            cpid, "Energy.Active.Import.Register", connector_id=2
        ) == pytest.approx(10.5)
        assert cs.get_metric(cpid, "Energy.Session", connector_id=2) == pytest.approx(
            0.5
        )
        assert (
            cs.get_unit(cpid, "Energy.Active.Import.Register", connector_id=2) == "kWh"
        )
        assert cs.get_unit(cpid, "Energy.Session", connector_id=2) == "kWh"

        ok = await cs.set_charger_state(
            cpid, csvcs.service_charge_start.name, True, connector_id=3
        )
        assert ok is True
        await asyncio.sleep(0.05)
        assert cp.last_start_evse_id == 2

    await run_charge_point_test(
        config_entry,
        "CP_v201_multi",
        ["ocpp2.0.1"],
        lambda ws: MultiConnectorChargePoint("CP_v201_multi_client", ws),
        [lambda cp: _scenario(hass, cs, cp)],
    )

    await remove_configuration(hass, config_entry)


async def _tx_event_eair(
    cp: MultiConnectorChargePoint,
    event_type: TransactionEventEnumType,
    evse_id: int,
    connector_id: int,
    tx_id: str,
    seq_no: int,
    eair_wh: int,
    context: ReadingContextEnumType | None = None,
    offline: bool | None = None,
):
    """Send a TransactionEvent carrying one Energy.Active.Import.Register sample."""
    sampled_value = {
        "measurand": "Energy.Active.Import.Register",
        "value": eair_wh,
        "unit_of_measure": {"unit": "Wh"},
    }
    if context is not None:
        sampled_value["context"] = context
    await cp.call(
        call.TransactionEvent(
            event_type=event_type,
            timestamp=datetime.now(UTC).isoformat(),
            trigger_reason=TriggerReasonEnumType.meter_value_periodic,
            seq_no=seq_no,
            transaction_info={
                "transaction_id": tx_id,
                "charging_state": ChargingStateEnumType.charging,
            },
            evse={"id": evse_id, "connector_id": connector_id},
            offline=offline,
            meter_value=[
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "sampled_value": [sampled_value],
                }
            ],
        )
    )


async def _run_session_energy_scenario(hass, cp_id: str, port: int, scenario):
    """Boot the three-connector charger and run a session-energy scenario."""
    config_data = copy.deepcopy(MOCK_CONFIG_DATA)
    config_data[CONF_CPIDS].append({cp_id: copy.deepcopy(MOCK_CONFIG_CP_APPEND)})
    config_data[CONF_CPIDS][-1][cp_id][CONF_CPID] = f"{cp_id}_cpid"
    config_data[CONF_PORT] = port
    config_entry = MockConfigEntry(
        domain=OCPP_DOMAIN,
        data=config_data,
        entry_id=cp_id,
        title=cp_id,
        version=2,
        minor_version=0,
    )
    cs = await create_configuration(hass, config_entry)
    ocpp.messages.ASYNC_VALIDATION = False

    async def _boot_and_run(cp: MultiConnectorChargePoint):
        boot = await cp.call(
            call.BootNotification(
                {
                    "serial_number": "SERIAL",
                    "model": "MODEL",
                    "vendor_name": "VENDOR",
                    "firmware_version": "VERSION",
                },
                BootReasonEnumType.power_up.value,
            )
        )
        assert boot.status == RegistrationStatusEnumType.accepted.value
        cp_srv = cs.charge_points[cp_id]
        await wait_ready(cp_srv)
        await asyncio.wait_for(cp.inventory_done.wait(), timeout=5)
        for _ in range(50):
            if getattr(cp_srv, "num_connectors", 0) == 3:
                break
            await asyncio.sleep(0.1)
        assert cp_srv.num_connectors == 3

        def session(connector_id: int):
            return cs.get_metric(
                cp_srv.settings.cpid, "Energy.Session", connector_id=connector_id
            )

        await scenario(cp, session)

    await run_charge_point_test(
        config_entry,
        cp_id,
        ["ocpp2.0.1"],
        lambda ws: MultiConnectorChargePoint(f"{cp_id}_client", ws),
        [_boot_and_run],
    )
    await remove_configuration(hass, config_entry)


@pytest.mark.timeout(60)
@pytest.mark.parametrize(
    ("start_wh", "anomaly_wh"),
    [
        # One sample below meter_start switches the mode (#2115).
        (356000, 8620),
        # A Started sample of 0 Wh switches it through meter_start == 0.
        (0, 8620),
    ],
    ids=["below_meter_start", "meter_start_zero"],
)
async def test_v201_session_energy_mode_does_not_outlive_its_transaction(
    hass, socket_enabled, start_wh, anomaly_wh
):
    """A session-energy detection must not decide how the next session is read.

    The charger reports a lifetime register. Something in the first
    transaction makes it look like a session-energy charger. The next
    transaction is a normal lifetime reading, so its session energy is
    EAIR - meter_start, not the raw register (#2143).
    """

    async def scenario(cp, session):
        started, updated, ended = (
            TransactionEventEnumType.started,
            TransactionEventEnumType.updated,
            TransactionEventEnumType.ended,
        )
        end = ReadingContextEnumType.transaction_end
        await _tx_event_eair(cp, started, 1, 1, "TX-A", 0, start_wh)
        await _tx_event_eair(cp, updated, 1, 1, "TX-A", 1, anomaly_wh)
        # The first transaction is read as session energy.
        assert session(1) == pytest.approx(8.62)
        await _tx_event_eair(cp, ended, 1, 1, "TX-A", 2, 8700, end)
        assert session(1) == pytest.approx(8.7)

        await _tx_event_eair(cp, started, 1, 1, "TX-B", 0, 356040)
        # The Started sample of the next transaction is its baseline, not
        # a session value of 356.04 kWh.
        assert session(1) == pytest.approx(0.0)
        await _tx_event_eair(cp, updated, 1, 1, "TX-B", 1, 357040)
        assert session(1) == pytest.approx(1.0), (
            f"session energy is the raw register, not EAIR - meter_start: {session(1)}"
        )
        await _tx_event_eair(cp, ended, 1, 1, "TX-B", 2, 357140, end)
        assert session(1) == pytest.approx(1.1)

    await _run_session_energy_scenario(
        hass,
        f"CP_v201_mode_per_tx_{start_wh}",
        9501 if start_wh else 9502,
        scenario,
    )


@pytest.mark.timeout(60)
async def test_v201_session_energy_mode_cleared_while_another_connector_runs(
    hass, socket_enabled
):
    """A start on one connector re-reads a session running on another.

    Connector 2 charges on a lifetime register while connector 1 has an
    anomalous sample. Once connector 1 starts its next transaction, connector
    2 is read as EAIR - meter_start again instead of as the raw register.
    """

    async def scenario(cp, session):
        started, updated, ended = (
            TransactionEventEnumType.started,
            TransactionEventEnumType.updated,
            TransactionEventEnumType.ended,
        )
        end = ReadingContextEnumType.transaction_end
        await _tx_event_eair(cp, started, 1, 2, "TX-2", 0, 356000)
        await _tx_event_eair(cp, started, 1, 1, "TX-1a", 0, 10000)
        await _tx_event_eair(cp, updated, 1, 1, "TX-1a", 1, 500)
        assert session(1) == pytest.approx(0.5)
        await _tx_event_eair(cp, ended, 1, 1, "TX-1a", 2, 600, end)

        await _tx_event_eair(cp, started, 1, 1, "TX-1b", 0, 10100)
        await _tx_event_eair(cp, updated, 1, 2, "TX-2", 1, 357000)
        assert session(2) == pytest.approx(1.0), (
            f"connector 2 reads the raw register: {session(2)}"
        )
        await _tx_event_eair(cp, ended, 1, 2, "TX-2", 2, 357100, end)
        assert session(2) == pytest.approx(1.1)

    await _run_session_energy_scenario(hass, "CP_v201_mode_two_conn", 9503, scenario)


@pytest.mark.timeout(60)
async def test_v201_session_energy_charger_is_re_detected_after_another_start(
    hass, socket_enabled
):
    """A charger that really reports session energy keeps its session value.

    Connector 1 reports session energy against a lifetime meter_start. A
    start on connector 3 clears the charger-wide mode mid-session; the next
    sample on connector 1 is re-detected, never derived as a negative.
    """

    async def scenario(cp, session):
        started, updated, ended = (
            TransactionEventEnumType.started,
            TransactionEventEnumType.updated,
            TransactionEventEnumType.ended,
        )
        end = ReadingContextEnumType.transaction_end
        await _tx_event_eair(cp, started, 1, 1, "TX-S", 0, 356000)
        await _tx_event_eair(cp, updated, 1, 1, "TX-S", 1, 500)
        assert session(1) == pytest.approx(0.5)

        await _tx_event_eair(cp, started, 2, 1, "TX-3", 0, 20000)
        await _tx_event_eair(cp, updated, 1, 1, "TX-S", 2, 600)
        assert session(1) == pytest.approx(0.6)
        await _tx_event_eair(cp, ended, 1, 1, "TX-S", 3, 700, end)
        assert session(1) == pytest.approx(0.7)

    await _run_session_energy_scenario(hass, "CP_v201_mode_redetect", 9504, scenario)


@pytest.mark.timeout(60)
async def test_v201_session_energy_mode_holds_for_the_rest_of_its_transaction(
    hass, socket_enabled
):
    """Only an online Started resets the mode, not every event.

    The charger reports session energy against a small lifetime meter_start,
    so its session value later climbs above meter_start. Neither a later
    Updated nor an offline Started replayed as history may switch the
    running session back to EAIR - meter_start.
    """

    async def scenario(cp, session):
        started, updated = (
            TransactionEventEnumType.started,
            TransactionEventEnumType.updated,
        )
        await _tx_event_eair(cp, started, 1, 1, "TX-S", 0, 5000)
        await _tx_event_eair(cp, updated, 1, 1, "TX-S", 1, 4900)
        assert session(1) == pytest.approx(4.9)
        await _tx_event_eair(cp, started, 1, 1, "TX-OLD", 0, 3000, offline=True)
        await _tx_event_eair(cp, updated, 1, 1, "TX-S", 2, 5100)
        assert session(1) == pytest.approx(5.1)

    await _run_session_energy_scenario(hass, "CP_v201_mode_holds", 9505, scenario)
