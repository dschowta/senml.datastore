package datastore

import (
	"encoding/json"
	"fmt"

	"github.com/cisco/senml"
	"github.com/dschowta/lite.tsdb"
)

type SenmlDataStore struct {
	tsdb tsdb.TSDB
}

type SenMLDBRecord struct {
	Unit        string   `json:"u,omitempty" `
	UpdateTime  float64  `json:"ut,omitempty"`
	Value       *float64 `json:"v,omitempty" `
	StringValue string   `json:"vs,omitempty" `
	DataValue   string   `json:"vd,omitempty"  `
	BoolValue   *bool    `json:"vb,omitempty" `

	Sum *float64 `json:"s,omitempty" `
}

func NewSenmlDataStore(storage int) (SenmlDataStore, error) {
	senmlDatastore := SenmlDataStore{}
	tsdb, err := tsdb.NewTSDB(storage)
	senmlDatastore.tsdb = tsdb
	return senmlDatastore, err
}

func (bdb SenmlDataStore) Connect(path string) error {
	return bdb.tsdb.Connect(path)
}

func (bdb SenmlDataStore) Disconnect() error {
	return bdb.tsdb.Disconnect()
}
func NewBoltSenMLRecord(record senml.SenMLRecord) SenMLDBRecord {
	return SenMLDBRecord{
		record.Unit,
		record.UpdateTime,
		record.Value,
		record.StringValue,
		record.DataValue,
		record.BoolValue,
		record.Sum,
	}
}

func newSenMLRecord(time float64, name string, record SenMLDBRecord) senml.SenMLRecord {
	return senml.SenMLRecord{
		Name:        name,
		Unit:        record.Unit,
		Time:        time,
		UpdateTime:  time,
		Value:       record.Value,
		StringValue: record.StringValue,
		DataValue:   record.DataValue,
		BoolValue:   record.BoolValue,
		Sum:         record.Sum,
	}
}

//This function converts a floating point number (which is supported by senml) to a bytearray
func floatTimeToInt64(senmlTime float64) int64 {
	//sec, frac := math.Modf(senmlTime)
	return int64(senmlTime * (1e9)) //time.Unix(int64(sec), int64(frac*(1e9))).UnixNano()
}

//This function converts a bytearray floating point number (which is supported by senml)
func int64ToFloatTime(timeVal int64) float64 {
	return float64(timeVal) / 1e9
}
func (bdb SenmlDataStore) Add(senmlPack senml.SenML) error {

	// Fill the data map with provided data points
	records := senml.Normalize(senmlPack).Records

	seriesMap := make(map[string][]tsdb.TimeEntry)
	for _, r := range records {
		if "" != r.Name {
			byte, err := json.Marshal(NewBoltSenMLRecord(r))
			if err != nil {
				return err
			}
			entry := tsdb.TimeEntry{floatTimeToInt64(r.Time), byte}

			seriesMap[r.Name] = append(seriesMap[r.Name], entry)
		} else {
			return fmt.Errorf("Senml record with Empty name")
		}

	}

	for name, series := range seriesMap {
		err := bdb.tsdb.Add(name, series)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bdb SenmlDataStore) Get(series string) (senml.SenML, error) {
	var senmlPack senml.SenML
	timeSeriesCh, errCh := bdb.tsdb.GetOnChannel(series)

	//Check the data channel
	for timeEntry := range timeSeriesCh {
		var timeRecord SenMLDBRecord
		err := json.Unmarshal(timeEntry.Value, &timeRecord)
		if err != nil {
			fmt.Printf("Error while unmarshalling %s", err)
			continue
		}
		senmlPack.Records = append(senmlPack.Records, newSenMLRecord(int64ToFloatTime(timeEntry.Time), series, timeRecord))
	}
	//Check the error channel
	err := <-errCh

	return senmlPack, err
}

func (bdb *SenmlDataStore) Delete(series string) error {
	return bdb.tsdb.Delete(series)
}
