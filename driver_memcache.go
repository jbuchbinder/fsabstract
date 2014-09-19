package fsabstract

import (
	"errors"
	memcache "github.com/bradfitz/gomemcache/memcache"
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
	Servers    string   `fsdconfig:"memcache.servers"`
	ServerList []string // populated by Servers

	conn *memcache.Client
}

func (self *FSMemcache) DriverName() string {
	return "fsdummy"
}

func (self *FSMemcache) Configure(c map[string]string) {
	if v, exists := c["memcache.servers"]; exists {
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

func (self *FSMemcache) Put(d FileStoreDescriptor, c []byte) error {
	// Create new location
	k := "fs_" + string(d.Id) + "_" + d.Name
	l := FileStoreLocation{
		Id:       "", // dummy driver doesn't have a store name/id
		Driver:   self.DriverName(),
		Created:  time.Now(),
		Location: k,
	}

	// Push out to filesystem
	err := self.conn.Set(&memcache.Item{Key: k, Value: c})
	if err != nil {
		return err
	}

	// Append location
	d.Location = append(d.Location, l)

	// No errors, send back
	return nil
}

func (self *FSMemcache) Delete(d FileStoreDescriptor, l FileStoreLocation) error {
	// Find the pertinent FileStoreLocation
	l, err := LocationForDriver(d, self.DriverName())
	if err != nil {
		return err
	}

	// Delete from disk
	err = self.conn.Delete(l.Location)
	if err != nil {
		return err
	}

	// Remove from mapping
	RemoveLocation(d, l)

	// No errors, send back
	return nil
}
