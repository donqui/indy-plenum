import pytest
import sys

from plenum.server.node import Node
from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_replies
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually


def test_different_last_ordered_on_backup_before_view_change(looper, txnPoolNodeSet,
                                                  sdk_pool_handle, sdk_wallet_client):
    ''' Send random request and do view change then fast_nodes (1, 4 - without
    primary backup replicas) are already ordered transaction on master and some backup replica
    and slow_nodes are not on backup replica. Wait ordering on slow_nodes.'''
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    slow_instance = 1
    slow_nodes = txnPoolNodeSet[1:3]
    fast_nodes = [n for n in txnPoolNodeSet if n not in slow_nodes]
    nodes_stashers = [n.nodeIbStasher for n in slow_nodes]
    old_last_ordered = txnPoolNodeSet[0].replicas[slow_instance].last_ordered_3pc
    with delay_rules(nodes_stashers, cDelay(delay=sys.maxsize,
                                            instId=slow_instance)):
        # send one  request
        requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                            sdk_wallet_client, 1)
        sdk_get_replies(looper, requests)
        old_view_no = txnPoolNodeSet[0].viewNo
        looper.run(
            eventually(last_ordered,
                       fast_nodes,
                       (old_view_no, old_last_ordered[1] + 1),
                       slow_instance))
        last_ordered_for_slow = slow_nodes[0].replicas[slow_instance].last_ordered_3pc

        # trigger view change on all nodes
        for node in txnPoolNodeSet:
            node.view_changer.on_master_degradation()

        # wait for view change done on all nodes
        looper.run(eventually(view_change_done, txnPoolNodeSet, old_view_no + 1))

    looper.run(
        eventually(last_ordered,
                   slow_nodes,
                   (old_view_no, last_ordered_for_slow[1] + 1),
                   slow_instance))


def test_different_last_ordered_on_master_before_view_change(looper, txnPoolNodeSet,
                                                  sdk_pool_handle, sdk_wallet_client):
    ''' Send random request and do view change then fast_nodes (1, 4 - without
    primary after next view change) are already ordered transaction on master
    and slow_nodes are not. Check ordering on slow_nodes.'''
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    master_instance = txnPoolNodeSet[0].master_replica.instId
    slow_nodes = txnPoolNodeSet[1:3]
    fast_nodes = [n for n in txnPoolNodeSet if n not in slow_nodes]
    nodes_stashers = [n.nodeIbStasher for n in slow_nodes]
    old_last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    with delay_rules(nodes_stashers, cDelay(delay=sys.maxsize)):
        # send one  request
        requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                             sdk_wallet_client, 1)
        last_ordered_for_slow = slow_nodes[0].master_replica.last_ordered_3pc
        old_view_no = txnPoolNodeSet[0].viewNo
        looper.run(
            eventually(last_ordered,
                       fast_nodes,
                       (old_view_no, old_last_ordered[1] + 1),
                       master_instance))

        # trigger view change on all nodes
        for node in txnPoolNodeSet:
            node.view_changer.on_master_degradation()

        # wait for view change done on all nodes
        looper.run(eventually(view_change_done, txnPoolNodeSet, old_view_no + 1))

    sdk_get_replies(looper, requests)
    looper.run(
        eventually(last_ordered,
                   slow_nodes,
                   (old_view_no, last_ordered_for_slow[1] + 1),
                   master_instance))


def last_ordered(nodes: [Node],
                 last_ordered,
                 instId):
    for node in nodes:
        assert node.replicas[instId].last_ordered_3pc == last_ordered


def view_change_done(nodes: [Node], view_no):
    for node in nodes:
        assert node.viewNo == view_no