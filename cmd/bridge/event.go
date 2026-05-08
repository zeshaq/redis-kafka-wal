package main

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// /api/event-state?region=us
//
// One-shot snapshot of the live-event app's state for one region:
// header, reactions, Q&A (top 20 by upvotes), poll, attendees count.
// The page polls this every ~800ms and writes via /api/produce, so this
// is the only read path the live UI needs.
const (
	maxQuestions = 20
	maxOptions   = 8
)

var reactionEmojis = []string{"heart", "fire", "clap", "laugh", "celebrate"}

type eventStateOut struct {
	Region    string         `json:"region"`
	Timestamp time.Time      `json:"timestamp"`
	Event     eventHeader    `json:"event"`
	Reactions map[string]int `json:"reactions"`
	QA        qaSection      `json:"qa"`
	Poll      pollSection    `json:"poll"`
}

type eventHeader struct {
	Title      string `json:"title"`
	Speaker    string `json:"speaker"`
	Topic      string `json:"topic"`
	Status     string `json:"status"`
	Attendees  int64  `json:"attendees"`
}

type qaSection struct {
	Total     int           `json:"total"`
	Questions []qaQuestion  `json:"questions"`
}

type qaQuestion struct {
	QID    string `json:"qid"`
	Text   string `json:"text"`
	Asker  string `json:"asker"`
	Region string `json:"region"`
	Votes  int64  `json:"votes"`
	TS     string `json:"ts"`
}

type pollSection struct {
	Question   string       `json:"question"`
	Options    []pollOption `json:"options"`
	TotalVotes int64        `json:"total_votes"`
}

type pollOption struct {
	Index int    `json:"index"`
	Text  string `json:"text"`
	Votes int64  `json:"votes"`
}

func (s *server) eventState(w http.ResponseWriter, r *http.Request) {
	region := r.URL.Query().Get("region")
	if region == "" {
		region = "us"
	}
	rdb, ok := s.rdb[region]
	if !ok {
		http.Error(w, "unknown region", http.StatusNotFound)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	out := eventStateOut{
		Region:    region,
		Timestamp: time.Now().UTC(),
		Event:     readEventHeader(ctx, rdb),
		Reactions: readReactions(ctx, rdb),
		QA:        readQA(ctx, rdb),
		Poll:      readPoll(ctx, rdb),
	}
	writeJSON(w, http.StatusOK, out)
}

func readEventHeader(ctx context.Context, rdb *redis.Client) eventHeader {
	get := func(k string) string { v, _ := rdb.Get(ctx, k).Result(); return v }
	att, _ := rdb.SCard(ctx, "event:attendees").Result()
	return eventHeader{
		Title:     orDefault(get("event:title"), "(no event configured)"),
		Speaker:   get("event:speaker"),
		Topic:     get("event:topic"),
		Status:    orDefault(get("event:status"), "draft"),
		Attendees: att,
	}
}

func readReactions(ctx context.Context, rdb *redis.Client) map[string]int {
	out := make(map[string]int, len(reactionEmojis))
	for _, e := range reactionEmojis {
		v, _ := rdb.Get(ctx, "event:reactions:"+e).Int()
		out[e] = v
	}
	return out
}

func readQA(ctx context.Context, rdb *redis.Client) qaSection {
	// Pull the most recent N questions from the stream, then enrich
	// each with its current upvote count, then sort by votes desc.
	xs, err := rdb.XRevRangeN(ctx, "event:qa:stream", "+", "-", 100).Result()
	if err != nil || len(xs) == 0 {
		return qaSection{Total: 0, Questions: nil}
	}
	qs := make([]qaQuestion, 0, len(xs))
	for _, x := range xs {
		v := x.Values
		qid, _ := v["qid"].(string)
		if qid == "" {
			qid = x.ID
		}
		text, _ := v["text"].(string)
		asker, _ := v["asker"].(string)
		reg, _ := v["region"].(string)
		votes, _ := rdb.Get(ctx, "event:qa:votes:"+qid).Int64()
		qs = append(qs, qaQuestion{
			QID: qid, Text: text, Asker: asker, Region: reg, Votes: votes, TS: x.ID,
		})
	}
	sort.SliceStable(qs, func(i, j int) bool {
		if qs[i].Votes != qs[j].Votes {
			return qs[i].Votes > qs[j].Votes
		}
		return qs[i].TS > qs[j].TS // newer first as tiebreak
	})
	if len(qs) > maxQuestions {
		qs = qs[:maxQuestions]
	}
	return qaSection{Total: len(xs), Questions: qs}
}

func readPoll(ctx context.Context, rdb *redis.Client) pollSection {
	get := func(k string) string { v, _ := rdb.Get(ctx, k).Result(); return v }
	question := get("event:poll:question")
	if question == "" {
		return pollSection{}
	}
	opts := make([]pollOption, 0, maxOptions)
	var total int64
	for i := 0; i < maxOptions; i++ {
		text := get("event:poll:option:" + strconv.Itoa(i))
		if text == "" {
			break
		}
		votes, _ := rdb.Get(ctx, "event:poll:tally:"+strconv.Itoa(i)).Int64()
		opts = append(opts, pollOption{Index: i, Text: text, Votes: votes})
		total += votes
	}
	return pollSection{Question: question, Options: opts, TotalVotes: total}
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
