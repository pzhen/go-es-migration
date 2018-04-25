// Elasticsearch 并发迁移程序
// 本地测试 50 并发 1w 数据 2.085481543s
package main

import (
	"fmt"
	"time"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"bytes"
	"math/rand"
	"sync"
	"strconv"
)

var (
	// 来源索引
	EsOrgIndexUser = "user"

	// 目标索引
	EsTargetIndexUser = "vmh_user_201804"

	// 目标日志索引
	EsTargetIndexErrLog = "vmh_user_201804_error_log"

	//wait goroutines
	Wg sync.WaitGroup

	// goroutines number
	ThreadsNum = 50

	RowsNum = 50

	// buffer
	Buffer = 100000

	// 来源机
	EsOrgServer = []string{
		"http://127.0.0.1:9200/",
		"http://127.0.0.1:9200/",
	}

	// 目标机
	EsTargetServer = []string{
		"http://127.0.0.1:9200/",
		"http://127.0.0.1:9200/",
	}
)

type (
	// ES 对象
	Elastic struct {
		Host      string
		IndexName string
		TypeName  string
		Path      string
	}

	// 消息体
	Response struct {
		ScrollId string `json:"_scroll_id"`
		Took     int    `json:"took"`
		TimedOut bool   `json:"timed_out"`

		Shards struct {
			Total      int `json:"total"`
			Successful int `json:"successful"`
			Failed     int `json:"failed"`
		} `json:"_shards"`

		Hits struct {
			Total    int     `json:"total"`
			MaxScore float64 `json:"max_score"`

			Hits [] struct {
				Index  string   `json:"_index"`
				Type   string   `json:"_type"`
				Id     string   `json:"_id"`
				Score  float64  `json:"_score"`
				Source UserInfo `json:"_source"`
			} `json:"hits"`
		}

		Error struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
			Phase  string `json:"phase"`
		} `json:"error"`
	}

	// 用户信息
	UserInfo struct {
		UserId        string `json:"user_id"`
		SinaUserId    string `json:"sina_user_id"`
		SinaNickname  string `json:"sina_nickname"`
		UserNickname  string `json:"user_nickname"`
		SinaAvatar    string `json:"sina_avatar"`
		Gender        string `json:"gender"`
		Credit        string `json:"credit"`
		IsVip         string `json:"is_vip"`
		BadgeIssuedV3 string `json:"badge_issued_v3"`
		Province      string `json:"province"`
		City          string `json:"city"`
		Location      string `json:"location"`
		Description   string `json:"description"`
		Verified      string `json:"verified"`
		VerifiedType  string `json:"verified_type"`
		UserLevel     string `json:"user_level"`
		Mbtype        string `json:"mbtype"`

		AddEsTime  int `json:"add_es_time"`
		UpdateTime int `json:"update_time"`
		CreateTime int `json:"create_time"`
		UserStatus int `json:"user_status"`
	}
)

func main() {
	t1 := time.Now()

	// data channel
	ch := make(chan UserInfo, Buffer)

	// produce
	go Produce(ch)

	// consume
	for task := 1; task <= ThreadsNum; task ++ {
		Wg.Add(1)
		go Consume(ch, task)
	}

	Wg.Wait()

	t2 := time.Since(t1)
	fmt.Println("run time : ", t2)
}

func debugLog(args ...interface{}) {
	if len(args) > 0 {
		fmt.Printf("%-10v\n", args)
	}
}

// 返回来 源机 Es 对象
func ElasticsearchOrg(indexName string, typeName string, path string) *Elastic {
	return &Elastic{
		Host:      EsOrgServer[rand.Intn(len(EsOrgServer))],
		IndexName: indexName,
		TypeName:  typeName,
		Path:      path,
	}
}

// 返回 目标机 Es 对象
func ElasticsearchTaget(indexName string, typeName string, path string) *Elastic {
	return &Elastic{
		Host:      EsTargetServer[rand.Intn(len(EsOrgServer))],
		IndexName: indexName,
		TypeName:  typeName,
		Path:      path,
	}
}

// es 通用
func (e *Elastic) ElasticsearchQuery(query interface{}) ([]byte, error) {
	url := e.Host

	if e.IndexName != "" {
		url += e.IndexName + "/"
	}

	if e.TypeName != "" {
		url += e.TypeName + "/"
	}

	if e.Path != "" {
		url += e.Path
	}

	qByte, _ := json.Marshal(query)
	return postHTTP(url, qByte)
}

func postHTTP(url string, data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	request, err := http.NewRequest("POST", url, reader)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Content-Type", "application/json;charset=UTF-8")
	client := http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// put data to channel
func Produce(ch chan UserInfo) {
	i := 0
	var response Response

	for {
		i += 1
		q := make(map[string]interface{})

		if i == 1 {
			q["size"] = RowsNum

			r, err := ElasticsearchOrg(EsOrgIndexUser, EsOrgIndexUser, "_search?scroll=10s").ElasticsearchQuery(q)
			if err != nil {
				debugLog("Produce", "error", err)
				break
			}

			json.Unmarshal(r, &response)

		} else {
			q["scroll"] = "10s"
			q["scroll_id"] = response.ScrollId

			r, err := ElasticsearchOrg("", "", "_search/scroll").ElasticsearchQuery(q)
			if err != nil {
				debugLog("Produce", "error", err)
				continue
			}

			json.Unmarshal(r, &response)
		}

		if len(response.Hits.Hits) == 0 {
			goto here
		}

		for n := 0; n <= len(response.Hits.Hits)-1; n++ {
			ch <- response.Hits.Hits[n].Source
		}
	}

here:
	close(ch)
	debugLog("Produce", "success", "channel", "produce task is over")
	return
}

// get data from channel
func Consume(ch chan UserInfo, task int) {
	defer Wg.Done()

	for v := range ch {
		// insert
		r, err := ElasticsearchTaget(EsTargetIndexUser, EsTargetIndexUser, v.UserId).ElasticsearchQuery(v)
		if err != nil {
			debugLog("Consume", "error", "user_id", v.UserId, err)
			continue
		}

		// result
		var res Response
		json.Unmarshal(r, &res)

		// error
		if res.Error.Type != "" {
			ElasticsearchTaget(EsTargetIndexErrLog, EsTargetIndexErrLog, v.UserId).ElasticsearchQuery(v)
			debugLog("Consume", "error", "user_id", v.UserId, res.Error.Reason)
			continue
		}

		debugLog("Consume"+strconv.Itoa(task), "success", "user_id", v.UserId)
	}

	debugLog("Consume"+strconv.Itoa(task), "success", task, "task is over")
	return
}
