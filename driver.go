package fsabstract

import (
	"fmt"
	"strings"
)

var (
	// FileStoreDriverMap is the internal driver mapping. Whenever a new
	// driver is declared in included code, it registers itself with this
	// mapping, which allows us to "autoload" drivers.
	FileStoreDriverMap = map[string]func() FileStoreDriver{}
)

type FileStoreDriver interface {
	DriverName() string
	Configure(map[string]string)
	Initialize() error
	Get(FileStoreDescriptor) ([]byte, FileStoreLocation, error)
	Put(FileStoreDescriptor, []byte) (FileStoreDescriptor, error)
	Delete(FileStoreDescriptor, FileStoreLocation) (FileStoreDescriptor, error)
}

func GetDriver(driverName string) FileStoreDriver {
	d := strings.TrimSpace(driverName)
	if _, exists := FileStoreDriverMap[d]; exists {
		return FileStoreDriverMap[d]()
	} else {
		fmt.Println("Unable to resolve driver " + d)
		return nil
	}
}
