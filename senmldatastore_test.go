package datastore

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/dschowta/lite.tsdb"

	"github.com/cisco/senml"
)

var filePath = "./testDb"
var datastore SenmlDataStore

func TestMain(m *testing.M) {
	setup_Test()
	retCode := m.Run()
	teardown_Test()
	os.Exit(retCode)
}

func setup_Test() {
	var err error
	datastore, err = NewSenmlDataStore(tsdb.BOLTDB)
	if err != nil {
		fmt.Printf("Unable to create senmlDataStore instance")
		os.Exit(1)
	}
	err = datastore.Connect(filePath)
	if err != nil {
		fmt.Printf("Unable to open the boltDB:%s", err)
		os.Exit(1)
	}
}

func teardown_Test() {
	datastore.Disconnect()
	err := os.Remove(filePath)
	if err != nil {
		fmt.Printf("Unable to delete the boltDB:%s", err)
		os.Exit(1)
	}
}

//TODO : Check whether a newly entered data will be sorted or not

func TestDataStore_Add(t *testing.T) {
	s := dummyRecords_same_name_same_types(10, "TestDataStore_Add", false)
	datastore.Add(s)
	s_normalized := senml.Normalize(s)
	seriesName := s_normalized.Records[0].Name
	s2, err := datastore.Get(seriesName)
	if err != nil {
		t.Error(err)
	}
	arr, err := senml.Encode(s2, senml.JSON, senml.OutputOptions{PrettyPrint: true})
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(arr))

	if CompareSenml(s_normalized, s2) == false {
		t.Error("Inserted and fetched senml did not match")
	}
}

func TestDataStore_Add_and_Check_Sorted(t *testing.T) {
	s := dummyRecords_same_name_same_types(10, "TestDataStore_Add_and_Check_Sorted", true)
	datastore.Add(s)
	s_normalized := senml.Normalize(s)

	fmt.Println("Created:")
	printJson(t, s_normalized)

	seriesName := s_normalized.Records[0].Name
	s2, err := datastore.Get(seriesName)
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Got Back:")
	printJson(t, s2)

	if CompareSenml(s_normalized, s2) == true {
		t.Error("Inserted and fetched senml was not supposed to match (sorted vs unsorted) ")
	}
	s_sorted := senml.Normalize(dummyRecords_same_name_same_types(10, "TestDataStore_Add_and_Check_Sorted", false))

	fmt.Println("sorted")
	printJson(t, s_sorted)

	if CompareSenml(s_sorted, s2) == false {
		t.Error("Sorted senml should have matched!!")
	}
}

func printJson(t *testing.T, ml senml.SenML) {
	arr, err := senml.Encode(ml, senml.JSON, senml.OutputOptions{PrettyPrint: true})
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(arr))
}
func TestDataStore_remove(t *testing.T) {
	s := dummyRecords_diff_name_diff_types()
	datastore.Add(s)
	seriesName := senml.Normalize(s).Records[0].Name
	_, err := datastore.Get(seriesName)
	if err != nil {
		t.Error(err)
	}

	//printJson(t,s2)

	err = datastore.Delete(seriesName)
	if err != nil {
		t.Error(err)
	}

	_, err = datastore.Get(seriesName)
	if err == nil {
		t.Error("Could fetch a deleted data")
	}
}

func dummyRecords_diff_name_diff_types() senml.SenML {

	value := 22.1
	sum := 0.0
	vb := true

	var s = senml.SenML{
		Records: []senml.SenMLRecord{
			{BaseName: "dev123",
				BaseTime:    -45.67,
				BaseUnit:    "degC",
				BaseVersion: 5,
				Value:       &value, Unit: "degC", Name: "temp", Time: -1.0, UpdateTime: 10.0, Sum: &sum},
			{StringValue: "kitchen", Name: "room", Time: -1.0},
			{DataValue: "abc", Name: "data"},
			{BoolValue: &vb, Name: "ok"},
		},
	}
	return s
}

func CompareSenml(s1 senml.SenML, s2 senml.SenML) bool {
	recordLen := len(s1.Records)
	for i := 0; i < recordLen; i++ {
		r1 := s1.Records[i]
		r2 := s2.Records[i]
		if compareRecords(r1, r2) == false {
			return false
		}
	}
	return true
}
func compareRecords(r1 senml.SenMLRecord, r2 senml.SenMLRecord) bool {
	return (math.Abs(r1.Time-r2.Time) < 1e-6 &&
		strings.Compare(r1.Name, r2.Name) == 0 &&
		strings.Compare(r1.DataValue, r2.DataValue) == 0 &&
		strings.Compare(r1.StringValue, r2.StringValue) == 0 &&
		((r1.Sum == nil && r2.Sum == nil) || *r1.Sum == *r2.Sum) &&
		((r1.BoolValue == nil && r2.BoolValue == nil) || *r1.BoolValue == *r2.BoolValue) &&
		((r1.Value == nil && r2.Value == nil) || *r1.Value == *r2.Value))
}

func dummyRecords_same_name_diff_types() senml.SenML {

	value := 22.1
	sum := 0.0
	vb := true

	var s = senml.SenML{
		Records: []senml.SenMLRecord{
			{BaseName: "dev123",
				BaseTime:    -45.67,
				BaseUnit:    "degC",
				BaseVersion: 5,
				Value:       &value, Unit: "degC", Name: "temp", Time: -1.0, UpdateTime: 10.0, Sum: &sum},
			{StringValue: "kitchen", Name: "temp", Time: -1.0},
			{DataValue: "abc", Name: "temp"},
			{BoolValue: &vb, Name: "temp"},
		},
	}
	return s
}

func dummyRecords_same_name_same_types(count int, name string, decremental bool) senml.SenML {

	value := 22.1
	timeinit := 1543059346.0
	mult := 1.0
	if decremental == true {
		timeinit = timeinit + float64(count-1)
		mult = -1.0
	}

	var s = senml.SenML{
		Records: []senml.SenMLRecord{
			{BaseName: "urn:dev:ow:10e2073a0108006:" + name,
				BaseUnit:    "A",
				BaseVersion: 5,
				Value:       &value, Name: "current", Time: timeinit},
		},
	}

	for i := 1.0; i < float64(count); i++ {
		s.Records = append(s.Records, senml.SenMLRecord{Value: &value, Name: "current", Time: (timeinit + i*mult)})
	}
	return s
}
