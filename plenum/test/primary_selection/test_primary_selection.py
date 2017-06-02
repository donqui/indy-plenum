import operator

import pytest

from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.primary_selection.helper import \
    check_rank_consistent_across_each_node
from stp_core.loop.eventually import eventually
from plenum.common.util import getNoInstances
from plenum.server.primary_selector import PrimarySelector
from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.test_node import checkProtocolInstanceSetup, getPrimaryReplica
from plenum.test.view_change.conftest import viewNo
from plenum.test.view_change.test_view_change import viewChangeDone

nodeCount = 7

# DEPR
# PrimaryDecider = PrimarySelector


@pytest.fixture()
def primaryReplicas(nodeSet, up):
    instanceCount = getNoInstances(nodeCount)
    return [getPrimaryReplica(nodeSet, i) for i in range(instanceCount)]


# noinspection PyIncorrectDocstring
def testPrimarySelectionAfterPoolReady(looper, nodeSet, ready, wallet1, client1):
    """
    Once the pool is ready(node has connected to at least 3 other nodes),
    appropriate primary replicas should be selected.
    """

    def checkPrimaryPlacement():
        # Node names sorted by rank
        sortedNodeNames = sorted(nodeSet.nodes.values(),
                                 key=operator.attrgetter("rank"))

        for idx, node in enumerate(sortedNodeNames):
            # For instance 0, the primary replica should be on the node with rank 0
            if idx == 0:
                Replica.generateName(sortedNodeNames[idx], 0)
                assert node.replicas[0].isPrimary
                assert not node.replicas[1].isPrimary
                assert not node.replicas[2].isPrimary

            # For instance 1, the primary replica should be on the node with rank 1
            if idx == 1:
                Replica.generateName(sortedNodeNames[idx], 1)
                assert not node.replicas[0].isPrimary
                assert node.replicas[1].isPrimary
                assert not node.replicas[2].isPrimary

            # For instance 2, the primary replica should be on the node with rank 2
            if idx == 2:
                Replica.generateName(sortedNodeNames[idx], 2)
                assert not node.replicas[0].isPrimary
                assert not node.replicas[1].isPrimary
                assert node.replicas[2].isPrimary

    check_rank_consistent_across_each_node(nodeSet)
    # Check if the primary is on the correct node
    timeout = waits.expectedPoolElectionTimeout(len(nodeSet))
    looper.run(eventually(checkPrimaryPlacement, retryWait=1, timeout=timeout))
    # Check if every protocol instance has one and only one primary and any node
    #  has no more than one primary
    checkProtocolInstanceSetup(looper, nodeSet, retryWait=1)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)


# noinspection PyIncorrectDocstring
def testPrimarySelectionAfterViewChange(looper, nodeSet, ready, primaryReplicas,
                                        viewChangeDone):
    """
    Test that primary replica of a protocol instance shifts to a new node after
    a view change.
    """

    # TODO: This test can fail due to view change.

    # Primary replicas before view change
    prBeforeVC = primaryReplicas

    # Primary replicas after view change
    instanceCount = getNoInstances(nodeCount)
    prAfterVC = [getPrimaryReplica(nodeSet, i) for i in range(instanceCount)]

    # Primary replicas have moved to the next node
    for br, ar in zip(prBeforeVC, prAfterVC):
        assert ar.node.rank - br.node.rank == 1

    check_rank_consistent_across_each_node(nodeSet)
    checkProtocolInstanceSetup(looper, nodeSet, retryWait=1)
