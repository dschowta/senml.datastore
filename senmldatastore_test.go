package datastore

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/farshidtz/senml"
)

func setupDatastore(dbName string) (*SenmlDataStore, string, error) {
	temp_file := fmt.Sprintf("senml_test_temp_%s", dbName)
	datastore := new(SenmlDataStore)

	err := datastore.Connect(temp_file)
	if err != nil {
		return nil, temp_file, err
	}
	return datastore, temp_file, nil
}

func clean(ds *SenmlDataStore, temp_filepath string) {
	err := ds.Disconnect()
	if err != nil {
		fmt.Println(err.Error())
	}
	err = os.Remove(temp_filepath)
	if err != nil {
		fmt.Println(err.Error())
	}
}

//TODO : Check whether a newly entered data will be sorted or not

func TestDataStore_Add(t *testing.T) {
	tname := "TestDataStore_Add"
	datastore, filePath, err := setupDatastore(tname)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer clean(datastore, filePath)

	s := dummyRecords_same_name_same_types(10, tname, false)
	err = datastore.Add(s)
	if err != nil {
		t.Error(err)
	}
	s_normalized := s.Normalize()
	seriesName := s_normalized[0].Name
	s2, err := datastore.Get(seriesName)
	if err != nil {
		t.Error(err)
	}
	_, err = s2.Encode(senml.JSON, senml.OutputOptions{PrettyPrint: true})
	if err != nil {
		t.Error(err)
	}
	//fmt.Println(string(arr))

	if compareSenml(s_normalized, s2) == false {
		t.Error("Inserted and fetched senml did not match")
	}
}

func TestDataStore_Add_and_Check_Sorted(t *testing.T) {
	tname := "TestDataStore_Add_and_Check_Sorted"
	datastore, filePath, err := setupDatastore(tname)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer clean(datastore, filePath)
	s := dummyRecords_same_name_same_types(10, tname, true)
	err = datastore.Add(s)
	if err != nil {
		t.Error(err)
	}
	s_normalized := s.Normalize()

	//fmt.Println("Created:")
	//printJson(t, s_normalized)

	seriesName := s_normalized[0].Name
	s2, err := datastore.Get(seriesName)
	if err != nil {
		t.Error(err)
	}

	//fmt.Println("Got Back:")
	//printJson(t, s2)

	if compareSenml(s_normalized, s2) == true {
		t.Error("Inserted and fetched senml was not supposed to match (sorted vs unsorted) ")
	}
	s_sorted := dummyRecords_same_name_same_types(10, "TestDataStore_Add_and_Check_Sorted", false)
	s_sorted = s_sorted.Normalize()
	//fmt.Println("sorted")
	//printJson(t, s_sorted)

	if compareSenml(s_sorted, s2) == false {
		t.Error("Sorted senml should have matched!!")
	}
}

/*func printJson(t *testing.T, ml senml.Pack) {
	arr, err := ml.Encode(senml.JSON, senml.OutputOptions{PrettyPrint: true})
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(arr))
}*/

func TestDataStore_remove(t *testing.T) {
	tname := "TestDataStore_remove"
	datastore, filePath, err := setupDatastore(tname)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer clean(datastore, filePath)
	s := dummyRecords_diff_name_diff_types()
	err = datastore.Add(s)
	if err != nil {
		t.Error(err)
	}
	seriesName := s.Normalize()[0].Name
	_, err = datastore.Get(seriesName)
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

func TestSenmlDataStore_QueryPages(t *testing.T) {
	tname := "TestSenmlDataStore_QueryPages"
	datastore, filePath, err := setupDatastore(tname)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer clean(datastore, filePath)

	s := dummyRecords_same_name_same_types(100, tname, false)

	s_normalized := s.Normalize()
	seriesName := s_normalized[0].Name

	limit := 25
	count := 100
	err = datastore.Add(s)
	if err != nil {
		t.Fatal(err)
	}

	query := Query{Series: seriesName, Limit: limit, Start: s[0].Time, End: s[len(s)-1].Time, Sort: ASC}
	pages, retCount, err := datastore.GetPages(query)
	if err != nil {
		t.Error(err)
	}

	if retCount != count {
		t.Errorf("Length of time series do not match, returned=%d, expected %d", retCount, count)
	}

	if len(pages) != count/limit {
		t.Error("Number of pages is not as expected")
	}

	for i := 0; i < count/limit; i = i + 1 {
		if math.Abs(pages[i]-s[i*limit].Time) > 1e-6 {
			t.Errorf("Page indices are not matching %v != %v", pages[i], s[i*limit].Time)
		}
	}
}

func TestSenmlDataStore_Query(t *testing.T) {
	tname := "TestSenmlDataStore_Query"
	datastore, filePath, err := setupDatastore(tname)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer clean(datastore, filePath)

	s := dummyRecords_same_name_same_types(100, tname, false)
	s_normalized := s.Normalize()
	seriesName := s_normalized[0].Name

	err = datastore.Add(s)
	if err != nil {
		t.Fatal(err)
	}

	query := Query{Series: seriesName, Limit: 50, Start: s[0].Time, End: s[len(s)-1].Time, Sort: ASC}
	resSeries, nextEntry, err := datastore.Query(query)
	if err != nil {
		t.Fatal(err)
	}

	firsthalf := s_normalized[0:50]
	if compareSenml(resSeries, firsthalf) == false {
		t.Error("First page entries did not match")
	}

	if nextEntry == nil {
		t.Error("nextEntry is null")
	}
	query = Query{Series: seriesName, Limit: 50, Start: *nextEntry, End: s[len(s)-1].Time, Sort: ASC}
	resSeries, nextEntry, err = datastore.Query(query)
	if err != nil {
		t.Fatal(err)
	}

	secondhalf := s_normalized[50:100]
	if compareSenml(resSeries, secondhalf) == false {
		t.Error("Second page entries did not match")
	}

	if nextEntry != nil {
		t.Error("nextEntry is not null")
	}
}

func dummyRecords_diff_name_diff_types() senml.Pack {

	value := 22.1
	sum := 0.0
	vb := true

	var s = []senml.Record{
		{BaseName: "dev123",
			BaseTime:    -45.67,
			BaseUnit:    "degC",
			BaseVersion: 5,
			Value:       &value, Unit: "degC", Name: "temp", Time: -1.0, UpdateTime: 10.0, Sum: &sum},
		{StringValue: "kitchen", Name: "room", Time: -1.0},
		{DataValue: "abc", Name: "data"},
		{BoolValue: &vb, Name: "ok"},
	}
	return s
}

func compareSenml(s1 senml.Pack, s2 senml.Pack) bool {
	recordLen := len(s1)
	for i := 0; i < recordLen; i++ {
		r1 := s1[i]
		r2 := s2[i]
		if compareRecords(r1, r2) == false {
			return false
		}
	}
	return true
}
func compareRecords(r1 senml.Record, r2 senml.Record) bool {
	return (math.Abs(r1.Time-r2.Time) < 1e-6 &&
		strings.Compare(r1.Name, r2.Name) == 0 &&
		strings.Compare(r1.DataValue, r2.DataValue) == 0 &&
		strings.Compare(r1.StringValue, r2.StringValue) == 0 &&
		((r1.Sum == nil && r2.Sum == nil) || *r1.Sum == *r2.Sum) &&
		((r1.BoolValue == nil && r2.BoolValue == nil) || *r1.BoolValue == *r2.BoolValue) &&
		((r1.Value == nil && r2.Value == nil) || *r1.Value == *r2.Value))
}

/*func dummyRecords_same_name_diff_types() senml.Pack {

	value := 22.1
	sum := 0.0
	vb := true

	var s = []senml.Record{
		{BaseName: "dev123",
			BaseTime:    -45.67,
			BaseUnit:    "degC",
			BaseVersion: 5,
			Value:       &value, Unit: "degC", Name: "temp", Time: -1.0, UpdateTime: 10.0, Sum: &sum},
		{StringValue: "kitchen", Name: "temp", Time: -1.0},
		{DataValue: "abc", Name: "temp"},
		{BoolValue: &vb, Name: "temp"},
	}
	return s
}*/

func dummyRecords_same_name_same_types(count int, name string, decremental bool) senml.Pack {

	value := 22.1
	timeinit := 1543059346.0
	mult := 1.0
	if decremental == true {
		timeinit = timeinit + float64(count-1)
		mult = -1.0
	}

	var s = []senml.Record{
		{BaseName: "urn:dev:ow:10e2073a0108006:" + name,
			BaseUnit:    "A",
			BaseVersion: 5,
			Value:       &value, Name: "current", Time: timeinit},
	}

	for i := 1.0; i < float64(count); i++ {
		s = append(s, senml.Record{Value: &value, Name: "current", Time: (timeinit + i*mult)})
	}
	return s
}
