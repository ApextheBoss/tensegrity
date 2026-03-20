/**
 * Example 3: Gossip-Based Service Discovery
 *
 * Demonstrates decentralized service discovery using gossip protocol
 * dissemination. Agents register services, discover each other through
 * gossip rounds, and route queries with health + locality awareness.
 *
 * No central registry needed — services propagate via epidemic gossip.
 *
 * Run: npx tsx examples/03-gossip-service-discovery.ts
 */

import {
  createGossipEngine,
  ServiceDiscoveryMesh,
} from 'tensegrity';

// ─── Part 1: Gossip Protocol Basics ─────────────────────────────────────────

function demoGossip() {
  console.log('=== Part 1: Gossip Protocol ===\n');

  // Create 3 gossip nodes using the small-cluster preset
  const node1 = createGossipEngine('node-1', 'small-cluster');
  const node2 = createGossipEngine('node-2', 'small-cluster');
  const node3 = createGossipEngine('node-3', 'small-cluster');

  // Join nodes into a cluster via SWIM membership
  const endpoints = [
    { id: 'node-1', address: 'localhost:8001', metadata: {}, generation: 1, heartbeat: 1 },
    { id: 'node-2', address: 'localhost:8002', metadata: {}, generation: 1, heartbeat: 1 },
    { id: 'node-3', address: 'localhost:8003', metadata: {}, generation: 1, heartbeat: 1 },
  ];

  node1.membership.addMember(endpoints[1]);
  node1.membership.addMember(endpoints[2]);
  node2.membership.addMember(endpoints[0]);
  node2.membership.addMember(endpoints[2]);
  node3.membership.addMember(endpoints[0]);
  node3.membership.addMember(endpoints[1]);

  console.log('  Nodes joined the cluster.');

  // Spread a rumor from node-1
  const rumor = node1.rumors.createRumor('service-announcement', {
    service: 'summarizer-v2',
    endpoint: 'http://node-1:3000/summarize',
  }, 10_000, 5);
  console.log(`  📢 Node-1 created rumor: ${rumor.id}\n`);

  // Simulate gossip rounds — in production, tick() runs on an interval
  const now = Date.now();
  for (let round = 0; round < 5; round++) {
    const t = now + round * 1000;
    const r1 = node1.tick(t);
    const r2 = node2.tick(t);
    const r3 = node3.tick(t);

    // In a real system, you'd send rumorPushes over the network.
    // Here we simulate delivery between nodes.
    for (const push of r1.rumorPushes) {
      for (const r of push.rumors) {
        const target = push.target === 'node-2' ? node2 : push.target === 'node-3' ? node3 : null;
        if (target) {
          target.rumors.receiveRumor({ ...r, hops: r.hops + 1 });
        }
      }
    }
    for (const push of r2.rumorPushes) {
      for (const r of push.rumors) {
        const target = push.target === 'node-1' ? node1 : push.target === 'node-3' ? node3 : null;
        if (target) {
          target.rumors.receiveRumor({ ...r, hops: r.hops + 1 });
        }
      }
    }
  }

  // Check rumor propagation stats
  const stats = node1.getStats();
  console.log(`  Cluster stats from node-1:`);
  console.log(`    Alive members: ${stats.aliveMembers}`);
  console.log(`    Active rumors: ${stats.activeRumors}`);
  console.log(`    Gossip ticks: ${stats.tickCount}`);
  console.log();
}

// ─── Part 2: Service Discovery Mesh ─────────────────────────────────────────

function demoServiceDiscovery() {
  console.log('=== Part 2: Service Discovery Mesh ===\n');

  // Create two mesh nodes (simulating two agents in different regions)
  const mesh1 = new ServiceDiscoveryMesh({
    nodeId: 'agent-east-1',
    defaultTtlMs: 30000,
    gossipFanout: 2,
  });

  const mesh2 = new ServiceDiscoveryMesh({
    nodeId: 'agent-west-1',
    defaultTtlMs: 30000,
    gossipFanout: 2,
  });

  // Connect peers so gossip can flow
  mesh1.addPeer('agent-west-1');
  mesh2.addPeer('agent-east-1');

  // Agent-east registers a summarization service
  mesh1.register({
    instanceId: 'summarizer-east-1',
    serviceType: 'agent.nlp',
    serviceName: 'summarizer',
    agentAddress: 'agent-east-1',
    endpoint: 'http://east-1:3000/summarize',
    version: '2.1.0',
    metadata: { model: 'gpt-4', maxTokens: '8192' },
    locality: { region: 'us-east', zone: 'us-east-1a' },
    priority: 5,
  });

  // Agent-west registers a translation service
  mesh2.register({
    instanceId: 'translator-west-1',
    serviceType: 'agent.nlp',
    serviceName: 'translator',
    agentAddress: 'agent-west-1',
    endpoint: 'http://west-1:3000/translate',
    version: '1.5.0',
    metadata: { languages: 'en,fr,de,es', model: 'nllb-200' },
    locality: { region: 'us-west', zone: 'us-west-2a' },
    priority: 3,
  });

  console.log('  📝 Registered services on each node.\n');

  // Simulate gossip exchange: mesh1 → mesh2 → mesh1
  const { digest: digest1 } = mesh1.initiateGossip();
  const delta1 = mesh2.handleGossipDigest(digest1);
  mesh1.applyGossipDelta(delta1, 'agent-west-1');

  const { digest: digest2 } = mesh2.initiateGossip();
  const delta2 = mesh1.handleGossipDigest(digest2);
  mesh2.applyGossipDelta(delta2, 'agent-east-1');

  console.log('  🔄 Gossip sync complete — nodes exchanged registrations.\n');

  // Now each node can discover services registered on the other
  const nlpFromEast = mesh1.discover({ serviceType: 'agent.nlp' });
  console.log(`  Agent-east discovers ${nlpFromEast.length} NLP service(s):`);
  for (const svc of nlpFromEast) {
    console.log(`    - ${svc.instance.serviceName} @ ${svc.instance.endpoint} (score: ${svc.score.toFixed(2)})`);
  }

  const nlpFromWest = mesh2.discover({ serviceType: 'agent.nlp' });
  console.log(`\n  Agent-west discovers ${nlpFromWest.length} NLP service(s):`);
  for (const svc of nlpFromWest) {
    console.log(`    - ${svc.instance.serviceName} @ ${svc.instance.endpoint} (score: ${svc.score.toFixed(2)})`);
  }

  // Query with attribute filter
  console.log('\n  🔍 Querying for NLP services with maxTokens metadata:');
  const highCapacity = mesh2.discover({
    serviceType: 'agent.nlp',
    attributes: { maxTokens: '8192' },
  });
  for (const svc of highCapacity) {
    console.log(`    - ${svc.instance.serviceName} (model: ${svc.instance.metadata.model})`);
  }

  // Health-aware discovery
  console.log('\n  🏥 Health-aware discovery (minHealth=0.5, maxLoad=0.8):');
  const healthy = mesh1.discover({
    serviceType: 'agent.nlp',
    minHealthScore: 0.5,
    maxLoad: 0.8,
    readyOnly: true,
  });
  console.log(`    Found ${healthy.length} healthy instance(s)`);

  console.log();
}

// ─── Run ─────────────────────────────────────────────────────────────────────

demoGossip();
demoServiceDiscovery();
