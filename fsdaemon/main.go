package main

import (
	"encoding/json"
	"flag"
	martini "github.com/go-martini/martini"
	fsabstract "github.com/jbuchbinder/fsabstract"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	DRIVER        = flag.String("driver", "dummy", "Driver")
	Driver        fsabstract.FileStoreDriver
	GlobalCounter int64
)

func main() {
	flag.Parse()

	GlobalCounter = 0

	// HACK! FIXME! TODO!
	c := make(map[string]string)
	c["fs.dummy.basepath"] = "." + string(os.PathSeparator) + "store"

	log.Print("Attempting to load driver " + *DRIVER)
	Driver = fsabstract.GetDriver(*DRIVER)
	if Driver == nil {
		panic("Invalid driver")
	}
	Driver.Configure(c)
	err := Driver.Initialize()
	if err != nil {
		panic(err)
	}

	m := martini.Classic()
	m.Get("/", func() string {
		return "Hello world!"
	})
	// Route storage requests properly
	m.Group("/resource", func(r martini.Router) {
		r.Get("/**", GetResource)
		//r.Post("/new", NewResource)
		r.Put("/new/:name", func(res http.ResponseWriter, req *http.Request) {
			log.Print("Got PUT request")
			// Sluck in
			name := "test"
			data, err := ioutil.ReadAll(req.Body)
			if err != nil {
				res.Write([]byte("ERROR"))
				res.WriteHeader(500) // HTTP 500
				return
			}
			res.Write([]byte(CreateResource(name, data)))
			res.WriteHeader(200) // HTTP 200
		})
		r.Delete("/:id", DeleteResource)
	})
	//http.Handle("/", m)
	m.Run()
}

func GetResource(params martini.Params) string {
	log.Print("Got GET request")
	f := params["_1"]
	var fsd fsabstract.FileStoreDescriptor
	err := json.Unmarshal([]byte(f), &fsd)
	if err != nil {
		log.Print(err)
		return ""
	}
	data, fsl, err := Driver.Get(fsd)
	log.Print("FSL : " + fsl.ToString())
	return string(data)
}

func DeleteResource(params martini.Params) string {
	log.Print("Got DELETE request")
	return "OK"
}

func CreateResource(name string, data []byte) string {
	// Create a simple file store descriptor. We do this because this
	// service is a simple implementation which does not support
	// multiple locations. Ideally, the FileStoreDescriptor would be
	// passed as part of the request, and a FileStoreLocation object
	// would be added.
	fsd := fsabstract.FileStoreDescriptor{
		Id:      GlobalCounter,
		Name:    name,
		Size:    int64(len(data)),
		Created: time.Now(),
	}

	fsd, err := Driver.Put(fsd, data)
	if err != nil {
		return "nil"
	}

	// Make sure to increment this. Horrible hack.
	GlobalCounter++

	return fsd.ToString()
}
