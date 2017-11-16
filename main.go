package journall

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/night-codes/types.v1"
	"reflect"
	"time"
)

type (
	obj map[string]interface{}
	arr []interface{}

	// Settings intended for data transmission into the Create method of package
	Settings struct {
		Name     string
		DB       *mgo.Database
		Interval time.Duration
		Indexes  []mgo.Index
	}

	JournalCollection struct {
		settings Settings
	}

	Query struct {
		queries []*mgo.Query
		skip    int
		limit   int
	}

	Inserter struct {
		ID  interface{}
		Doc interface{}
	}
)

// Create new panel
func Create(s Settings) *JournalCollection {
	return &JournalCollection{
		settings: s,
	}
}

func (jc *JournalCollection) Insert(insert ...Inserter) error {
	tm := int64(time.Now().UnixNano() / int64(jc.settings.Interval))
	idColl := jc.settings.DB.C(jc.settings.Name + "-IDS")
	collection := jc.settings.DB.C(jc.settings.Name + "-" + types.String(tm))

	for _, index := range jc.settings.Indexes {
		index.Background = true
		collection.EnsureIndex(index)
	}
	docs := []interface{}{}
	ids := []interface{}{}
	for _, ins := range insert {
		if ins.ID != nil {
			docs = append(docs, ins.Doc)
			ids = append(ids, obj{"_id": ins.ID, "tm": tm})
		}
	}
	if len(ids) > 0 {
		if err := idColl.Insert(ids...); err != nil {
			fmt.Println(err)
		}
		return collection.Insert(docs...)
	}
	return nil
}

func (jc *JournalCollection) FindId(id interface{}) *mgo.Query {
	idColl := jc.settings.DB.C(jc.settings.Name + "-IDS")
	var tm int64
	result := obj{}
	cursor := idColl.FindId(id)

	if err := cursor.One(&result); err == nil {
		if tmi, exist := result["tm"]; exist {
			var ok bool
			if tm, ok = tmi.(int64); !ok {
				fmt.Println("Not OK")
			}
		}
	} else {
		fmt.Println(err)
	}
	return jc.settings.DB.C(jc.settings.Name + "-" + types.String(tm)).FindId(id)
}

func (jc *JournalCollection) Update(insert Inserter, query interface{}, recordTime time.Time) error {
	tm := recordTime.UnixNano() / int64(jc.settings.Interval)
	collection := jc.settings.DB.C(jc.settings.Name + "-" + types.String(tm))
	return collection.Update(query, insert.Doc)
}

func (jc *JournalCollection) Find(query interface{}, TimeFrom time.Time, TimeTo ...time.Time) *Query {
	if len(TimeTo) == 0 {
		TimeTo = append(TimeTo, time.Now())
	}
	timeTo := TimeTo[0].UnixNano() / int64(jc.settings.Interval)
	timeFrom := TimeFrom.UnixNano() / int64(jc.settings.Interval)
	queries := []*mgo.Query{}

	if timeFrom > timeTo {
		timeFrom, timeTo = timeTo, timeFrom
	}
	for tm := timeTo; tm >= timeFrom; tm-- {
		collection := jc.settings.DB.C(jc.settings.Name + "-" + types.String(tm))
		queries = append(queries, collection.Find(query).Sort("-$natural"))
	}
	return &Query{queries: queries}
}

func (q *Query) All(result interface{}) {
	varA := reflect.ValueOf(result)
	skip := q.skip
	limit := q.limit

	if varA.Kind() != reflect.Ptr { // должен быть указатель
		return
	}
	varA = varA.Elem()                // берем значение по указателю
	if varA.Kind() != reflect.Slice { // и это значение должно быть slice
		return
	}

	varB := reflect.MakeSlice(varA.Type(), 0, 0)
	for _, query := range q.queries {
		count, _ := query.Count()
		if count <= skip {
			skip -= count
		} else { //skip < count
			query.Skip(skip).Limit(limit).All(result)
			varB = reflect.AppendSlice(varB, varA)
			limit -= varA.Len()
			if limit == 0 {
				break
			}
			skip = 0
		}
	}
	varA.Set(varB)
}

func (q *Query) One(result interface{}) (err error) {
	for _, query := range q.queries {
		if n, error := query.Count(); error == nil && n > 0 {
			err = query.One(result)
			return
		}
	}
	return mgo.ErrNotFound
}

func (q *Query) Count() (n int, err error) {
	for _, query := range q.queries {
		if nn, error := query.Count(); error == nil {
			n += nn
		} else {
			err = error
			return
		}
	}
	return
}

func (q *Query) Distinct(key string, result interface{}) {
	varA := reflect.ValueOf(result)
	if varA.Kind() != reflect.Ptr { // должен быть указатель
		return
	}
	varA = varA.Elem()                // берем значение по указателю
	if varA.Kind() != reflect.Slice { // и это значение должно быть slice
		return
	}

	varB := reflect.MakeSlice(varA.Type(), 0, 0)
	for _, query := range q.queries {
		query.Distinct(key, result)
		fmt.Println(key, result)
		varB = reflect.AppendSlice(varB, varA)
	}
	varA.Set(varB)
}

func (q *Query) Select(selector interface{}) *Query {
	for k, query := range q.queries {
		q.queries[k] = query.Select(selector)
	}
	return q
}

func (q *Query) Skip(n uint64) *Query {
	q.skip = int(n)
	return q
}

func (q *Query) Limit(n uint64) *Query {
	q.limit = int(n)
	return q
}
