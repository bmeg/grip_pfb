package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bmeg/grip/gripper"
	"github.com/linkedin/goavro/v2"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/go-plugin"
)

func avroTransform(d interface{}) interface{} {
	if x, ok := d.(map[string]interface{}); ok {
		if len(x) == 1 {
			if v, ok := x["string"]; ok {
				return v
			}
			if v, ok := x["float"]; ok {
				return v
			}
		}
		out := map[string]interface{}{}
		for k, v := range x {
			out[k] = avroTransform(v)
		}
		return out
	}
	return d
}

func getObjectMap(i map[string]interface{}, key string) (map[string]interface{}, bool) {
	if k, ok := i[key]; ok {
		if o, ok := k.(map[string]interface{}); ok {
			return o, true
		}
	}
	return nil, false
}

func getObjectArray(i map[string]interface{}, key string) ([]interface{}, bool) {
	if k, ok := i[key]; ok {
		if o, ok := k.([]interface{}); ok {
			return o, true
		}
	}
	return nil, false
}

func getObjectString(i map[string]interface{}, key string) (string, bool) {
	if k, ok := i[key]; ok {
		if o, ok := k.(string); ok {
			return o, true
		}
	}
	return "", false
}

type ElementDriver struct {
	data map[string]map[string]interface{}
}

func (ed *ElementDriver) AddEntity(key string, value map[string]interface{}) {
	ed.data[key] = value
}

func (ed *ElementDriver) GetRows() map[string]*gripper.BaseRow {
	out := map[string]*gripper.BaseRow{}
	for key, data := range ed.data {
		o := gripper.BaseRow{Key: key, Value: data}
		out[key] = &o
	}
	return out
}

func loadTables(avroPath string) (map[string]*ElementDriver, map[string]map[string]string, error) {
	fh, err := os.Open(avroPath)
	if err != nil {
		log.Printf("Issues\n")
		return nil, nil, err
	}

	ocf, err := goavro.NewOCFReader(fh)
	if err != nil {
		log.Printf("Issues Reading File: %s\n", err)
		return nil, nil, err
	}

	fieldLinkMap := map[string]map[string]string{}
	tables := map[string]*ElementDriver{}
	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			log.Printf("Issues Reading File: %s\n", err)
			return nil, nil, err
		}
		t := avroTransform(datum)
		if record, ok := t.(map[string]interface{}); ok {
			id := record["id"]
			name := record["name"]
			if id == nil && name == "Metadata" {
				if object, ok := getObjectMap(record, "object"); ok {
					if metadata, ok := getObjectMap(object, "Metadata"); ok {
						if nodes, ok := getObjectArray(metadata, "nodes"); ok {
							for _, node := range nodes {
								if nodeData, ok := node.(map[string]interface{}); ok {
									if nodeName, ok := getObjectString(nodeData, "name"); ok {
										log.Printf("table: %s\n", nodeName)
										nodeDriver := ElementDriver{data: map[string]map[string]interface{}{}}
										tables[nodeName] = &nodeDriver
										if links, ok := getObjectArray(nodeData, "links"); ok {
											for _, link := range links {
												if linkData, ok := link.(map[string]interface{}); ok {
													dst := linkData["dst"]
													//linkName := linkData["name"]
													//fmt.Printf("link: %s - %s > %s\n", nodeName, linkName, dst)
													edgeTableName := fmt.Sprintf("%s:%s", nodeName, dst)
													nodeDriver := ElementDriver{data: map[string]map[string]interface{}{}}
													log.Printf("EdgeTable: %s\n", edgeTableName)
													tables[edgeTableName] = &nodeDriver
													fieldLinkMap[edgeTableName] = map[string]string{}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			} else {
				if recordID, ok := getObjectString(record, "id"); ok {
					if recName, ok := getObjectString(record, "name"); ok {
						if object, ok := getObjectMap(record, "object"); ok {
							if entity, ok := getObjectMap(object, recName); ok {
								tables[recName].AddEntity(recordID, entity)
							}
						}
						if links, ok := getObjectArray(record, "relations"); ok {
							for _, link := range links {
								if linkData, ok := link.(map[string]interface{}); ok {
									if dstName, ok := getObjectString(linkData, "dst_name"); ok {
										if dstID, ok := getObjectString(linkData, "dst_id"); ok {
											edgeID := fmt.Sprintf("%s:%s", recordID, dstID)
											edgeTableName := fmt.Sprintf("%s:%s", recName, dstName)
											tables[edgeTableName].AddEntity(edgeID, map[string]interface{}{recName: recordID, dstName: dstID})
											fieldLinkMap[edgeTableName][recName] = recName
											fieldLinkMap[edgeTableName][dstName] = dstName
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	return tables, fieldLinkMap, nil
}

func printGraph(tables map[string]*ElementDriver) {
	mapVertices := map[string]interface{}{}
	for t := range tables {
		if !strings.Contains(t, ":") {
			mapVertices[t+"/"] = map[string]interface{}{"source": "pfb", "label": t, "collection": t}
		}
	}
	mapEdges := map[string]interface{}{}
	for t := range tables {
		if strings.Contains(t, ":") {
			tmp := strings.Split(t, ":")
			edgeTable := map[string]interface{}{"source": "pfb", "collection": t, "fromField": "$." + tmp[0], "toField": "$." + tmp[1]}
			mapEdges[fmt.Sprintf("%s-%s", tmp[0], tmp[1])] = map[string]interface{}{
				"fromVertex": tmp[0] + "/",
				"toVertex":   tmp[1] + "/",
				"label":      tmp[1],
				"edgeTable":  edgeTable,
			}
		}
	}
	sc, _ := json.Marshal(map[string]interface{}{"vertices": mapVertices, "edges": mapEdges})

	fmt.Printf("%s\n", sc)
}

func main() {
	flag.Parse()
	configPath := flag.Args()[0]

	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return
	}

	config := map[string]string{}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return
	}

	var avroPath string
	if t, ok := config["path"]; !ok {
		log.Printf("No path found")
		return
	} else {
		avroPath = t
	}
	tables, fieldLinkMap, err := loadTables(avroPath)
	if err != nil {
		return
	}

	drivers := map[string]gripper.Driver{}
	for t, v := range tables {
		if fm, ok := fieldLinkMap[t]; ok {
			log.Printf("LinkMap: %s", fm)
			drivers[t] = gripper.NewDriverPreload(v.GetRows(), fm)
		} else {
			drivers[t] = gripper.NewDriverPreload(v.GetRows(), nil)
		}
	}

	srv := gripper.NewSimpleTableServer(drivers)
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: gripper.Handshake,
		Plugins: map[string]plugin.Plugin{
			"gripper": &gripper.GripPlugin{Impl: srv},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
