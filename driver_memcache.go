package fsabstract

import (
	"errors"
	memcache "github.com/bradfitz/gomemcache/memcache"
	"strconv"
	"strings"
	"time"
)

func init() {
	FileStoreDriverMap["memcache"] = func() FileStoreDriver {
		return new(FSMemcache)
	}
}

// FSMemcache is a memcache filesystem driver. It sets a series of servers,
// separated by commas, for the memcache instances. This is a terrible idea
// for anything other than testing, since memcache doesn't persist anywhere
// besides memory.
type FSMemcache struct {
	Servers    string   `fsdconfig:"fs.memcache.servers"`
	ServerList []string // populated by Servers

	conn *memcache.Client
}

func (self *FSMemcache) DriverName() string {
	return "memcache"
}

func (self *FSMemcache) Configure(c map[string]string) {
	if v, exists := c["fs.memcache.servers"]; exists {
		self.Servers = v
		if v != "" {
			self.ServerList = strings.Split(self.Servers, ",")
			// Normalize any spaces out of the way
			for k, v := range self.ServerList {
				self.ServerList[k] = strings.TrimSpace(v)
			}
		}
	}
}

func (self *FSMemcache) Initialize() error {
	self.conn = memcache.New(self.ServerList...)
	if self.conn == nil {
		return errors.New("Unable to initialize memcache driver")
	}
	return nil
}

func (self *FSMemcache) Get(d FileStoreDescriptor) ([]byte, FileStoreLocation, error) {
	// Find the pertinent FileStoreLocation
	l, err := LocationForDriver(d, self.DriverName())
	if err != nil {
		return nil, FileStoreLocation{}, err
	}

	// Retrieve actual file data from disk
	c, err := self.conn.Get(l.Location)
	if err != nil {
		return nil, l, err
	}

	// Send everything back
	return c.Value, l, nil
}

func (self *FSMemcache) Put(d FileStoreDescriptor, c []byte) (FileStoreDescriptor, error) {
	dU := d

	// Create new location
	k := "fs_" + strconv.FormatInt(dU.Id, 16) + "_" + dU.Name
	l := FileStoreLocation{
		Id:       "", // dummy driver doesn't have a store name/id
		Driver:   self.DriverName(),
		Created:  time.Now(),
		Location: k,
	}

	// Push out to filesystem
	err := self.conn.Set(&memcache.Item{Key: k, Value: c})
	if err != nil {
		return dU, err
	}

	// Append location
	if dU.Location == nil {
		dU.Location = make([]FileStoreLocation, 0)
	}
	dU.Location = append(dU.Location, l)

	// No errors, send back
	return dU, nil
}

func (self *FSMemcache) Delete(d FileStoreDescriptor, l FileStoreLocation) (FileStoreDescriptor, error) {
	dU := d

	// Find the pertinent FileStoreLocation
	l, err := LocationForDriver(dU, self.DriverName())
	if err != nil {
		return dU, err
	}

	// Delete from disk
	err = self.conn.Delete(l.Location)
	if err != nil {
		return dU, err
	}

	// Remove from mapping
	RemoveLocation(dU, l)

	// No errors, send back
	return dU, nil
}
