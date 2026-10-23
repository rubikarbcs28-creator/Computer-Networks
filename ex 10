#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"
#include "ns3/flow-monitor-module.h"
#include "ns3/netanim-module.h"

#include <fstream>
#include <iostream>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("Ns2TclPort");

static Ptr<PacketSink> g_tcpSink;
static std::ofstream g_throughput;
static uint64_t g_lastTcpRx = 0;
static const double g_sampleInterval = 1.0;

static void
RecordThroughput()
{
    const double now = Simulator::Now().GetSeconds();
    const uint64_t currentRx =
        (g_tcpSink != nullptr) ? g_tcpSink->GetTotalRx() : 0;
    const uint64_t bytesReceived = currentRx - g_lastTcpRx;
    const double throughputMbps =
        bytesReceived * 8.0 / g_sampleInterval / 1e6;

    g_throughput << now << "," << throughputMbps << "\n";
    g_lastTcpRx = currentRx;

    if (now + g_sampleInterval <= 4.5)
    {
        Simulator::Schedule(Seconds(g_sampleInterval), &RecordThroughput);
    }
}

static void
SetLinkState(Ptr<Node> node, Ptr<NetDevice> device,
             bool up, const std::string label)
{
    Ptr<Ipv4> ipv4 = node->GetObject<Ipv4>();
    int32_t interface = ipv4->GetInterfaceForDevice(device);

    if (interface < 0)
    {
        NS_LOG_ERROR("Could not find IPv4 interface for " << label);
        return;
    }

    if (up)
    {
        ipv4->SetUp(static_cast<uint32_t>(interface));
        std::cout << Simulator::Now().GetSeconds()
                  << " s: link " << label << " set UP\n";
    }
    else
    {
        ipv4->SetDown(static_cast<uint32_t>(interface));
        std::cout << Simulator::Now().GetSeconds()
                  << " s: link " << label << " set DOWN\n";
    }

    // Recalculate global routes after a topology change.
    Ipv4GlobalRoutingHelper::RecomputeRoutingTables();
}

int
main(int argc, char *argv[])
{
    double simTime = 5.0;
    double udpRateMbps = 1.0;
    uint32_t queuePackets = 10;

    CommandLine cmd(__FILE__);
    cmd.AddValue("simTime", "Simulation duration in seconds", simTime);
    cmd.AddValue("udpRate", "UDP CBR rate in Mbps", udpRateMbps);
    cmd.AddValue("queuePackets", "Queue limit in packets", queuePackets);
    cmd.Parse(argc, argv);

    // Use TCP NewReno, corresponding to a common classic TCP setup.
    Config::SetDefault("ns3::TcpL4Protocol::SocketType",
                       TypeIdValue(TcpNewReno::GetTypeId()));

    // Nodes: n0 (TCP source), n1 (router/UDP source),
    // n2 and n3 (routers), n4 (common receiver).
    NodeContainer nodes;
    nodes.Create(5);

    Ptr<Node> n0 = nodes.Get(0);
    Ptr<Node> n1 = nodes.Get(1);
    Ptr<Node> n2 = nodes.Get(2);
    Ptr<Node> n3 = nodes.Get(3);
    Ptr<Node> n4 = nodes.Get(4);

    InternetStackHelper internet;
    internet.Install(nodes);

    // Link n0-n1: 2 Mbps, 10 ms.
    PointToPointHelper link01;
    link01.SetDeviceAttribute("DataRate", StringValue("2Mbps"));
    link01.SetChannelAttribute("Delay", StringValue("10ms"));
    NetDeviceContainer d01 = link01.Install(n0, n1);

    // Link n1-n2: 1.9 Mbps, 15 ms.
    PointToPointHelper link12;
    link12.SetDeviceAttribute("DataRate", StringValue("1.9Mbps"));
    link12.SetChannelAttribute("Delay", StringValue("15ms"));
    NetDeviceContainer d12 = link12.Install(n1, n2);

    // Link n2-n3: 1.7 Mbps, 20 ms; queue limit 10 packets.
    PointToPointHelper link23;
    link23.SetDeviceAttribute("DataRate", StringValue("1.7Mbps"));
    link23.SetChannelAttribute("Delay", StringValue("20ms"));
    link23.SetQueue("ns3::DropTailQueue<Packet>",
                    "MaxSize",
                    StringValue(std::to_string(queuePackets) + "p"));
    NetDeviceContainer d23 = link23.Install(n2, n3);

    // Link n3-n4: 1.7 Mbps, 20 ms.
    PointToPointHelper link34;
    link34.SetDeviceAttribute("DataRate", StringValue("1.7Mbps"));
    link34.SetChannelAttribute("Delay", StringValue("20ms"));
    NetDeviceContainer d34 = link34.Install(n3, n4);

    // Direct link n4-n1: 2 Mbps, 12 ms.
    PointToPointHelper link41;
    link41.SetDeviceAttribute("DataRate", StringValue("2Mbps"));
    link41.SetChannelAttribute("Delay", StringValue("12ms"));
    NetDeviceContainer d41 = link41.Install(n4, n1);

    // Assign a separate subnet to each link.
    Ipv4AddressHelper address;

    address.SetBase("10.1.1.0", "255.255.255.0");
    address.Assign(d01);

    address.SetBase("10.1.2.0", "255.255.255.0");
    address.Assign(d12);

    address.SetBase("10.1.3.0", "255.255.255.0");
    address.Assign(d23);

    address.SetBase("10.1.4.0", "255.255.255.0");
    address.Assign(d34);

    address.SetBase("10.1.5.0", "255.255.255.0");
    Ipv4InterfaceContainer if41 = address.Assign(d41);

    Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    // TCP receiver at n4.
    const uint16_t tcpPort = 5000;
    PacketSinkHelper tcpSinkHelper(
        "ns3::TcpSocketFactory",
        InetSocketAddress(Ipv4Address::GetAny(), tcpPort));

    ApplicationContainer tcpSinkApps = tcpSinkHelper.Install(n4);
    tcpSinkApps.Start(Seconds(0.0));
    tcpSinkApps.Stop(Seconds(simTime));

    // UDP receiver at n4.
    const uint16_t udpPort = 5001;
    PacketSinkHelper udpSinkHelper(
        "ns3::UdpSocketFactory",
        InetSocketAddress(Ipv4Address::GetAny(), udpPort));

    ApplicationContainer udpSinkApps = udpSinkHelper.Install(n4);
    udpSinkApps.Start(Seconds(0.0));
    udpSinkApps.Stop(Seconds(simTime));

    // TCP source at n0: BulkSend approximates the NS-2 FTP application.
    BulkSendHelper tcpSource(
        "ns3::TcpSocketFactory",
        InetSocketAddress(if41.GetAddress(0), tcpPort));
    tcpSource.SetAttribute("MaxBytes", UintegerValue(0));
    tcpSource.SetAttribute("SendSize", UintegerValue(1000));

    ApplicationContainer tcpSourceApps = tcpSource.Install(n0);
    tcpSourceApps.Start(Seconds(1.0));
    tcpSourceApps.Stop(Seconds(4.0));

    // UDP CBR source at n1, directed to n4's address on the direct link.
    OnOffHelper udpSource(
        "ns3::UdpSocketFactory",
        InetSocketAddress(if41.GetAddress(0), udpPort));
    udpSource.SetAttribute(
        "DataRate",
        DataRateValue(DataRate(static_cast<uint64_t>(udpRateMbps * 1e6))));
    udpSource.SetAttribute("PacketSize", UintegerValue(1000));
    udpSource.SetAttribute(
        "OnTime",
        StringValue("ns3::ConstantRandomVariable[Constant=1]"));
    udpSource.SetAttribute(
        "OffTime",
        StringValue("ns3::ConstantRandomVariable[Constant=0]"));

    ApplicationContainer udpSourceApps = udpSource.Install(n1);
    udpSourceApps.Start(Seconds(0.1));
    udpSourceApps.Stop(Seconds(4.5));

    // Preserve the Tcl script's intended n1-n4 link interruption:
    // down at 1.0 s and up at 2.0 s. The original "up at 0.1 s"
    // is unnecessary because the link starts up.
    Simulator::Schedule(Seconds(1.0), &SetLinkState,
                        n1, d41.Get(1), false, std::string("n1-n4"));
    Simulator::Schedule(Seconds(2.0), &SetLinkState,
                        n1, d41.Get(1), true, std::string("n1-n4"));

    // TCP throughput sampled once per second, in Mbps.
    g_tcpSink = DynamicCast<PacketSink>(tcpSinkApps.Get(0));
    g_throughput.open("tcp-throughput.csv");
    g_throughput << "Time(s),TCP_Goodput(Mbps)\n";
    Simulator::Schedule(Seconds(1.5), &RecordThroughput);

    // Packet captures for Wireshark.
    link01.EnablePcap("n0-n1", d01.Get(0), true);
    link12.EnablePcap("n1-n2", d12.Get(0), true);
    link23.EnablePcap("n2-n3", d23.Get(0), true);
    link34.EnablePcap("n3-n4", d34.Get(0), true);
    link41.EnablePcap("n1-n4", d41.Get(1), true);

    // NetAnim visualization: create an XML trace and arrange nodes in a ring.
    // Open nsls-animation.xml with the NetAnim GUI after the simulation.
    AnimationInterface anim("nsls-animation.xml");
    anim.SetMaxPktsPerTraceFile(500000);
    anim.SetConstantPosition(n0, 10.0, 20.0);
    anim.SetConstantPosition(n1, 30.0, 10.0);
    anim.SetConstantPosition(n2, 50.0, 20.0);
    anim.SetConstantPosition(n3, 50.0, 40.0);
    anim.SetConstantPosition(n4, 30.0, 50.0);
    anim.UpdateNodeDescription(n0, "n0 TCP source");
    anim.UpdateNodeDescription(n1, "n1 UDP source/router");
    anim.UpdateNodeDescription(n2, "n2 router");
    anim.UpdateNodeDescription(n3, "n3 router");
    anim.UpdateNodeDescription(n4, "n4 receiver");
    anim.UpdateNodeColor(n0, 0, 80, 220);
    anim.UpdateNodeColor(n1, 220, 80, 0);
    anim.UpdateNodeColor(n2, 80, 160, 80);
    anim.UpdateNodeColor(n3, 80, 160, 80);
    anim.UpdateNodeColor(n4, 140, 60, 180);

    // FlowMonitor statistics.
    FlowMonitorHelper flowHelper;
    Ptr<FlowMonitor> monitor = flowHelper.InstallAll();

    Simulator::Stop(Seconds(simTime));
    Simulator::Run();

    Ptr<PacketSink> udpSink =
        DynamicCast<PacketSink>(udpSinkApps.Get(0));

    const double tcpRxMbps =
        tcpSink->GetTotalRx() * 8.0 / simTime / 1e6;
    const double udpRxMbps =
        udpSink->GetTotalRx() * 8.0 / simTime / 1e6;

    std::cout << "\n===== NS-3 TCP/UDP SIMULATION RESULTS =====\n";
    std::cout << "Simulation duration: " << simTime << " s\n";
    std::cout << "UDP configured rate: " << udpRateMbps << " Mbps\n";
    std::cout << "TCP received bytes: " << tcpSink->GetTotalRx() << "\n";
    std::cout << "UDP received bytes: " << udpSink->GetTotalRx() << "\n";
    std::cout << "TCP average goodput over full run: "
              << tcpRxMbps << " Mbps\n";
    std::cout << "UDP average goodput over full run: "
              << udpRxMbps << " Mbps\n";

    monitor->CheckForLostPackets();
    monitor->SerializeToXmlFile("flowmon-results.xml", true, true);

    Ptr<Ipv4FlowClassifier> classifier =
        DynamicCast<Ipv4FlowClassifier>(flowHelper.GetClassifier());

    for (const auto &entry : monitor->GetFlowStats())
    {
        Ipv4FlowClassifier::FiveTuple tuple =
            classifier->FindFlow(entry.first);
        const FlowMonitor::FlowStats &stats = entry.second;

        std::cout << "\nFlow " << entry.first
                  << ": " << tuple.sourceAddress << " -> "
                  << tuple.destinationAddress
                  << "\n  Tx packets: " << stats.txPackets
                  << "\n  Rx packets: " << stats.rxPackets
                  << "\n  Lost packets: " << stats.lostPackets
                  << "\n  Mean delay (s): "
                  << (stats.rxPackets
                      ? stats.delaySum.GetSeconds() / stats.rxPackets
                      : 0.0)
                  << "\n";
    }

    g_throughput.close();
    Simulator::Destroy();
    return 0;
}
