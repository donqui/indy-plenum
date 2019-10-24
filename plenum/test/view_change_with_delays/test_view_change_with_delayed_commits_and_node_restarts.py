import pytest

from plenum.test import waits
from plenum.test.delayers import cDelay
from plenum.test.helper import perf_monitor_disabled, sdk_send_random_request, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    """
    Patch config so that monitor won't start view change unexpectedly
    """
    with perf_monitor_disabled(tconf):
        yield tconf


def check_last_ordered(nodes, num):
    for node in nodes:
        assert node.master_replica.last_ordered_3pc == num


def view_change_done(nodes, view_no):
    for node in nodes:
        assert node.viewNo == view_no


def test_view_change_with_delayed_commits_on_one_node_and_restarts_of_others(txnPoolNodeSet, looper, sdk_pool_handle,
                                                                             sdk_wallet_client, tconf, tdir,
                                                                             allPluginsPath):
    """
    Bla bla black sheep.
    """

    slow_node = txnPoolNodeSet[-1]
    fast_nodes = [node for node in txnPoolNodeSet if node != slow_node]

    view_no = slow_node.viewNo
    old_last_ordered = slow_node.master_replica.last_ordered_3pc

    with delay_rules_without_processing(slow_node.nodeIbStasher, cDelay()):
        # Send requests
        sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

        # Check that all of the nodes except the slow one ordered the request
        looper.run(eventually(check_last_ordered, fast_nodes, (view_no, old_last_ordered[1] + 1)))
        looper.run(eventually(check_last_ordered, [slow_node], old_last_ordered))

    # Restart fast nodes
    for node in fast_nodes:
        disconnect_node_and_ensure_disconnected(
            looper,
            txnPoolNodeSet,
            node,
            timeout=len(fast_nodes),
            stopNode=True
        )
        looper.removeProdable(node)
        txnPoolNodeSet.remove(node)

        restarted_node = start_stopped_node(node, looper, tconf, tdir, allPluginsPath)
        txnPoolNodeSet.append(restarted_node)

    looper.runFor(waits.expectedNodeStartUpTimeout())
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Trigger view change on all nodes
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()

    assert len(txnPoolNodeSet) == 4

    # Assert that view change was successful and that ledger data is consistent
    waitForViewChange(
        looper,
        txnPoolNodeSet,
        expectedViewNo=(view_no + 1),
        customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(txnPoolNodeSet))
    )
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


def test_view_change_with_delayed_commits_on_multiple_nodes_and_restarts_of_others(txnPoolNodeSet, looper,
                                                                                   sdk_pool_handle, sdk_wallet_client,
                                                                                   tconf, tdir, allPluginsPath):
    """
    Bla bla black sheep.
    """

    slow_nodes = txnPoolNodeSet[-2:]
    slow_stashers = [slow_node.nodeIbStasher for slow_node in slow_nodes]
    fast_nodes = [node for node in txnPoolNodeSet if node not in slow_nodes]

    view_no = slow_nodes[0].viewNo
    old_last_ordered = slow_nodes[0].master_replica.last_ordered_3pc

    with delay_rules_without_processing(slow_stashers, cDelay()):
        # Send requests
        sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

        # Check that all of the nodes except the slows one ordered the request
        looper.run(eventually(check_last_ordered, fast_nodes, (view_no, old_last_ordered[1] + 1)))
        looper.run(eventually(check_last_ordered, slow_nodes, old_last_ordered))

        # Restart fast nodes
        for node in fast_nodes:
            disconnect_node_and_ensure_disconnected(
                looper,
                txnPoolNodeSet,
                node,
                timeout=len(fast_nodes),
                stopNode=True
            )
            looper.removeProdable(node)
            txnPoolNodeSet.remove(node)

            restarted_node = start_stopped_node(node, looper, tconf, tdir, allPluginsPath)
            txnPoolNodeSet.append(restarted_node)

        looper.runFor(waits.expectedNodeStartUpTimeout())
        looper.run(checkNodesConnected(txnPoolNodeSet))

    # Trigger view change on all nodes
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()

    assert len(txnPoolNodeSet) == 4

    # Assert that view change was successful and that ledger data is consistent
    waitForViewChange(
        looper,
        txnPoolNodeSet,
        expectedViewNo=(view_no + 1),
        customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(txnPoolNodeSet))
    )
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


def test_view_change_with_delayed_commits_on_multiple_nodes_and_restarts_of_others_123(txnPoolNodeSet, looper,
                                                                                   sdk_pool_handle, sdk_wallet_client,
                                                                                   tconf, tdir, allPluginsPath):
    """
    Bla bla black sheep.
    """

    slow_nodes = txnPoolNodeSet[1:]
    slow_stashers = [slow_node.nodeIbStasher for slow_node in slow_nodes]
    fast_nodes = [node for node in txnPoolNodeSet if node not in slow_nodes]

    view_no = slow_nodes[0].viewNo
    old_last_ordered = slow_nodes[0].master_replica.last_ordered_3pc

    with delay_rules_without_processing(slow_stashers, cDelay()):
        # Send requests
        sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

        # Check that all of the nodes except the slow ones ordered the request
        looper.run(eventually(check_last_ordered, fast_nodes, (view_no, old_last_ordered[1] + 1)))
        looper.run(eventually(check_last_ordered, slow_nodes, old_last_ordered))

    # Restart slow nodes
    for node in slow_nodes:
        disconnect_node_and_ensure_disconnected(
            looper,
            txnPoolNodeSet,
            node,
            timeout=len(slow_nodes),
            stopNode=True
        )
        looper.removeProdable(node)
        txnPoolNodeSet.remove(node)

        restarted_node = start_stopped_node(node, looper, tconf, tdir, allPluginsPath)
        txnPoolNodeSet.append(restarted_node)

    looper.runFor(waits.expectedNodeStartUpTimeout())
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Trigger view change on all nodes
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()

    assert len(txnPoolNodeSet) == 4

    # Assert that view change was successful and that ledger data is consistent
    waitForViewChange(
        looper,
        txnPoolNodeSet,
        expectedViewNo=(view_no + 1),
        customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(txnPoolNodeSet))
    )

    looper.runFor(10)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
