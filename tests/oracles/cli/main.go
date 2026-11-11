// Apache-2.0 LiveKit commands.go rendering body, unchanged except writer target.
package main
import("bytes";"encoding/hex";"encoding/json";"fmt";"os";"strconv";"strings";"time";"github.com/dustin/go-humanize";"github.com/olekukonko/tablewriter";"github.com/olekukonko/tablewriter/tw";"github.com/livekit/protocol/livekit";"github.com/jxskiss/base62";"google.golang.org/protobuf/encoding/protojson")
func render(nodes []*livekit.Node) string { var out bytes.Buffer
	table := tablewriter.NewTable(&out,
		tablewriter.WithHeaderAutoFormat(tw.Off),
		tablewriter.WithRowAutoWrap(0),
		tablewriter.WithRendition(tw.Rendition{
			Settings: tw.Settings{
				Separators: tw.Separators{BetweenRows: tw.On},
			},
		}),
		tablewriter.WithRowAlignmentConfig(tw.CellAlignment{
			PerColumn: []tw.Align{
				tw.AlignCenter, tw.AlignCenter, tw.AlignCenter,
				tw.AlignRight, tw.AlignRight,
				tw.AlignRight,
				tw.AlignRight, tw.AlignRight,
				tw.AlignRight, tw.AlignRight, tw.AlignRight,
				tw.AlignRight, tw.AlignRight,
				tw.AlignCenter,
			},
		}),
	)
	table.Header(
		"ID", "IP Address", "Region",
		"CPUs", "CPU Usage\nLoad Avg",
		"Memory Used/Total",
		"Rooms", "Clients\nTracks In/Out",
		"Bytes/s In/Out\nBytes Total", "Packets/s In/Out\nPackets Total", "System Dropped Pkts/s\nPkts/s Out/Dropped",
		"Nack/s\nNack Total", "Retrans/s\nRetrans Total",
		"Started At\nUpdated At",
	)

	for _, node := range nodes {
		stats := node.Stats
		rate := &livekit.NodeStatsRate{}
		if len(stats.Rates) > 0 {
			rate = stats.Rates[0]
		}

		// Id and state
		idAndState := fmt.Sprintf("%s\n(%s)", node.Id, node.State.Enum().String())

		// System stats
		cpus := strconv.Itoa(int(stats.NumCpus))
		cpuUsageAndLoadAvg := fmt.Sprintf("%.2f %%\n%.2f %.2f %.2f", stats.CpuLoad*100,
			stats.LoadAvgLast1Min, stats.LoadAvgLast5Min, stats.LoadAvgLast15Min)
		memUsage := fmt.Sprintf("%s / %s", humanize.Bytes(stats.MemoryUsed), humanize.Bytes(stats.MemoryTotal))

		// Room stats
		rooms := strconv.Itoa(int(stats.NumRooms))
		clientsAndTracks := fmt.Sprintf("%d\n%d / %d", stats.NumClients, stats.NumTracksIn, stats.NumTracksOut)

		// Packet stats
		bytes := fmt.Sprintf("%sps / %sps\n%s / %s", humanize.Bytes(uint64(rate.BytesIn)), humanize.Bytes(uint64(rate.BytesOut)),
			humanize.Bytes(stats.BytesIn), humanize.Bytes(stats.BytesOut))
		packets := fmt.Sprintf("%s / %s\n%s / %s", humanize.Comma(int64(rate.PacketsIn)), humanize.Comma(int64(rate.PacketsOut)),
			strings.TrimSpace(humanize.SIWithDigits(float64(stats.PacketsIn), 2, "")), strings.TrimSpace(humanize.SIWithDigits(float64(stats.PacketsOut), 2, "")))
		sysPacketsDroppedPct := float32(0)
		if rate.SysPacketsOut+rate.SysPacketsDropped > 0 {
			sysPacketsDroppedPct = float32(rate.SysPacketsDropped) / float32(rate.SysPacketsDropped+rate.SysPacketsOut)
		}
		sysPackets := fmt.Sprintf("%.2f %%\n%v / %v", sysPacketsDroppedPct*100, float64(rate.SysPacketsOut), float64(rate.SysPacketsDropped))
		nacks := fmt.Sprintf("%.2f\n%s", rate.NackTotal, strings.TrimSpace(humanize.SIWithDigits(float64(stats.NackTotal), 2, "")))
		retransmit := fmt.Sprintf("%.2f\n%s", rate.RetransmitPacketsOut, strings.TrimSpace(humanize.SIWithDigits(float64(stats.RetransmitPacketsOut), 2, "")))

		// Date
		startedAndUpdated := fmt.Sprintf("%s\n%s", time.Unix(stats.StartedAt, 0).UTC().UTC().Format("2006-01-02 15:04:05"),
			time.Unix(stats.UpdatedAt, 0).UTC().Format("2006-01-02 15:04:05"))

		table.Append(
			idAndState, node.Ip, node.Region,
			cpus, cpuUsageAndLoadAvg,
			memUsage,
			rooms, clientsAndTracks,
			bytes, packets, sysPackets,
			nacks, retransmit,
			startedAndUpdated,
		)
	}
	table.Render() //nolint:errcheck

return out.String() }
func main(){ decoder:=json.NewDecoder(os.Stdin);encoder:=json.NewEncoder(os.Stdout);for{var row struct{Name string `json:"name"`; Hex *string `json:"hex"`; Nodes []json.RawMessage `json:"nodes"`};if err:=decoder.Decode(&row);err!=nil{return};if row.Hex!=nil{data,err:=hex.DecodeString(*row.Hex);if err!=nil{panic(err)};_ = encoder.Encode(map[string]any{"name":row.Name,"encoded":base62.EncodeToString(data)});continue};nodes:=make([]*livekit.Node,0,len(row.Nodes));for _,raw:=range row.Nodes{n:=new(livekit.Node);if err:=protojson.Unmarshal(raw,n);err!=nil{panic(err)};nodes=append(nodes,n)};_ = encoder.Encode(map[string]any{"name":row.Name,"table":render(nodes)})}}
