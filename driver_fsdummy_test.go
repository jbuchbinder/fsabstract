package fsabstract

import (
	//      "errors"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestDummyDriver(t *testing.T) {
	t.Log("Testing dummy file store driver")

	// Create configuration mapping
	c := make(map[string]string)
	c["fs.dummy.basepath"] = "." + string(os.PathSeparator) + "drivertest"
	t.Log(c)

	// Load driver
	t.Log("Load dummy driver")
	d := GetDriver("dummy")
	if d == nil {
		t.Error("Unable to instantiate dummy file store driver")
		return
	}
	t.Log("Configure()")
	d.Configure(c)

	t.Log("Initialize()")
	err := d.Initialize()
	if err != nil {
		t.Error(err)
		return
	}

	// Dummy test data
	filedata := []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04}
	fsd := FileStoreDescriptor{
		Id:      1,
		Name:    "testfile.bin",
		Size:    8,
		Created: time.Now(),
	}

	t.Log("Put()")
	fsd, err = d.Put(fsd, filedata)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("fsd = " + fsd.ToString())

	t.Log("Get()")
	data, fsl, err := d.Get(fsd)
	if err != nil {
		t.Error(err)
		t.Log("fsd = " + fsd.ToString())
		return
	}
	if len(data) != len(filedata) {
		t.Errorf("len(data) == %d, len(filedata) == %d", len(data), len(filedata))
		t.Log("fsd = " + fsd.ToString())
		return
	}
	if !reflect.DeepEqual(data, filedata) {
		t.Error("data != filedata")
		t.Log("fsd = " + fsd.ToString())
		return
	}
	t.Log(fsl.ToString())
	t.Log("fsd = " + fsd.ToString())

	t.Log("Delete()")
	fsd, err = d.Delete(fsd, fsl)
	if err != nil {
		t.Error(err)
		return
	}

	t.Log("Cleanup")
	os.Remove(c["fs.dummy.basepath"])

	t.Log("Completed dummy file store driver tests")
}
