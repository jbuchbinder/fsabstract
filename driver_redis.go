package fsabstract

import (
	"errors"
	redis "github.com/jbuchbinder/go-redis"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	REDIS_READONLY  = false
	REDIS_READWRITE = true
)

func init() {
	FileStoreDriverMap["redis"] = func() FileStoreDriver {
		return new(FSRedis)
	}
}

// FSRedis is a Redis filesystem driver. It sets a series of servers,
// separated by commas, for the Redis instances.
type FSRedis struct {
	RwServer     string   `fsdconfig:"fs.redis.server"`
	RoServers    string   `fsdconfig:"fs.redis.slaveServers"`
	RoServerList []string // populated by RoServers
}

type redisConnection struct {
	host     string
	port     int
	password string
	db       int
	connspec *redis.ConnectionSpec
}

func (self *FSRedis) DriverName() string {
	return "redis"
}

func (self *FSRedis) Configure(c map[string]string) {
	if v, exists := c["fs.redis.server"]; exists {
		self.RwServer = v
	}
	if v, exists := c["fs.redis.slaveServers"]; exists {
		self.RoServers = v
		if v != "" {
			self.RoServerList = strings.Split(self.RoServers, ",")
			// Normalize any spaces out of the way
			for k, v := range self.RoServerList {
				self.RoServerList[k] = strings.TrimSpace(v)
			}
		}
	}
}

func (self *FSRedis) Initialize() error {
	return nil
}

func (self *FSRedis) Get(d FileStoreDescriptor) ([]byte, FileStoreLocation, error) {
	// RO connection
	conn, err := redis.NewSynchClientWithSpec(self.getConnection(REDIS_READONLY).connspec)
	if err != nil {
		return nil, FileStoreLocation{}, err
	}

	// Find the pertinent FileStoreLocation
	l, rerr := LocationForDriver(d, self.DriverName())
	if rerr != nil {
		return nil, FileStoreLocation{}, errors.New(err.Error())
	}

	// Retrieve actual file data from disk
	c, err := conn.Get(l.Location)
	if err != nil {
		return nil, l, err
	}

	// Send everything back
	return c, l, nil
}

func (self *FSRedis) Put(d FileStoreDescriptor, c []byte) (FileStoreDescriptor, error) {
	dU := d

	// RW connection
	conn, err := redis.NewSynchClientWithSpec(self.getConnection(REDIS_READWRITE).connspec)
	if err != nil {
		return dU, err
	}

	// Create new location
	k := "fs_" + strconv.FormatInt(d.Id, 16) + "_" + dU.Name
	l := FileStoreLocation{
		Id:       self.RwServer, // store server name, in case of migration
		Driver:   self.DriverName(),
		Created:  time.Now(),
		Location: k,
	}

	// Push out to filesystem
	err = conn.Set(k, c)
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

func (self *FSRedis) Delete(d FileStoreDescriptor, l FileStoreLocation) (FileStoreDescriptor, error) {
	dU := d

	// RW connection
	conn, err := redis.NewSynchClientWithSpec(self.getConnection(REDIS_READWRITE).connspec)
	if err != nil {
		return dU, err
	}

	// Delete from disk
	_, err = conn.Del(l.Location)
	if err != nil {
		return dU, err
	}

	// Remove from mapping
	RemoveLocation(dU, l)

	// No errors, send back
	return dU, nil
}

func (self *FSRedis) getConnection(write bool) redisConnection {
	var c redisConnection

	if write || len(self.RoServerList) < 1 {
		c.host, c.port, c.db, c.password = self.parseRedisUrl(self.RwServer)
	} else {
		// TODO: FIXME: XXX: Pick something other than the first one
		c.host, c.port, c.db, c.password = self.parseRedisUrl(self.RoServerList[0])
	}

	if c.password != "" {
		c.connspec = redis.DefaultSpec().Host(c.host).Port(c.port).Db(c.db).Password(c.password)
	} else {
		c.connspec = redis.DefaultSpec().Host(c.host).Port(c.port).Db(c.db)
	}

	return c
}

func (self *FSRedis) parseRedisUrl(rurl string) (host string, port int, db int, password string) {
	// (If there's an error, use default info)

	// scheme://[userinfo@]host/path[?query][#fragment]
	// example:
	//	redis://password@host:port/db
	purl, err := url.Parse(rurl)
	if err != nil {
		log.Print("Could not parse redis URL : " + rurl + "[" + err.Error() + "]")
		return "127.0.0.1", 6379, 1, ""
	}

	host = purl.Host
	port = 6379 // default
	if strings.Contains(host, ":") {
		parts := strings.Split(host, ":")
		host = parts[0]
		port, err = strconv.Atoi(parts[1])
		if err != nil {
			port = 6379 // default to default port
		}
	}
	db, err = strconv.Atoi(purl.Path)
	if err != nil {
		db = 1 // default
	}

	return host, port, db, purl.User.String()
}
