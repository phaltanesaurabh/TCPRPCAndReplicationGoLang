package main

import (
        "fmt"
        "github.com/drone/routes"
        "net/http"
        "log"
        "os"
        "encoding/json"
        "github.com/mkilling/goejdb"
        "labix.org/v2/mgo/bson"
        "net/rpc"
        "net"
        "io/ioutil"
        "github.com/naoina/toml"
        "strconv"
        "strings"
)


//Struct to Parse the TOML Input Configuration 

type serverConfig struct {
  Database struct{
    File_name string
    Port_num int
  }
  
  Replication struct{
    Rpc_server_port_num int
    Replica []string
  }
  
}

var config serverConfig
var db_name string
var port_num string
var rpc_server_port_num string
var replica string
var service string



var jb *goejdb.Ejdb
var coll *goejdb.EjColl


//Define Status for RPC Handlers

var Success string = "0"
var Failure string = "1"
var Error string = "2"

type Request struct{
Request_payload string
Request_emailid string 

}

type Response struct{
Response_code string
Response_payload string

}


// Do the Insert,Update,Delete,POST with Databases 
//READ the Database Configuration Externally from the TOML File 
//Replicate the Database to other Instance



type Req int

func record_exists(emailid string) int {
  email_string:=`{"email" : "`+ emailid +`"}`
  if v , _ := coll.Find(email_string); len(v) >0 {
                      return 1
                  } else {
                       return 0
                  }
}


//RPC POST handler

func (t *Req) Post_Handler(req *Request, resp *Response) error {

    b := []byte(req.Request_payload)
          
    var f interface{}
    err := json.Unmarshal(b, &f)

    m := f.(map[string]interface{})


   if (record_exists(req.Request_emailid)==0){
  
             if err != nil {
                                 os.Exit(1)
                          }
                bsrec, _ := bson.Marshal(m)
                coll.SaveBson(bsrec)
                fmt.Printf("\nSaved data")

           
  //Sample Debug Code to determine the posted 
             res, _ := coll.Find(`{"email" : {"$begin" : "foo"}}`) // Name starts with 'Bru' string
                          fmt.Printf("\n\nRecords found: %d\n", len(res))
                          for _, bs := range res {
                              var m map[string]interface{}
                              bson.Unmarshal(bs, &m)
                              fmt.Println(m)

                          }

        resp.Response_payload="Success"
        resp.Response_code=Success                 
              
       }else{ 
 
        resp.Response_payload="Failure"
        resp.Response_code=Failure 

       }

    return nil
}


//RPC Update handler
func (t *Req) Update_Handler(req *Request, resp *Response) error {



   if (record_exists(req.Request_emailid)==1){
  
      

        coll.Update(`{"email":"`+string(req.Request_emailid)+`","$set":`+req.Request_payload+`}`)               
     

        resp.Response_payload="Success"
        resp.Response_code=Success                 
              
       }else{ 
 
        resp.Response_payload="Failure"
        resp.Response_code=Failure 

       }

    return nil
}



//RPC Delete handler

func (t *Req) Delete_Handler(req *Request,resp *Response) error{

if (record_exists(req.Request_emailid)==1){

    

       email_string:=`{"email" : "`+ string(req.Request_emailid) +`"}`


      res, _ := coll.Find(email_string) 
      
      for _, bs := range res {
                             var m map[string]interface{}
                             bson.Unmarshal(bs, &m)
                             res:=coll.RmBson((m["_id"].(bson.ObjectId)).Hex())
                               log.Println(res)

                          } 

    fmt.Println("%s %s","Profile Deleted",string(email_string))  
    resp.Response_payload="Success"
    resp.Response_code=Success      
  
  }else{

      //return Failure or Record Not Found 
      // Handle Error and Send Error Code 

    resp.Response_payload="Failure"
    resp.Response_code=Failure 

  }

return nil
}



//Client POST Profile 

func postprofile(w http.ResponseWriter, r *http.Request) {
   
         var f interface{}
         json.NewDecoder(r.Body).Decode(&f)
         fmt.Println(f)
         m := f.(map[string]interface{})

          ouput,_:=json.Marshal(m)

         fmt.Println("This is string",string(ouput))


    //var service = replica
    fmt.Println("Tnisisithe server: ",service)
    var client, err1 = rpc.Dial("tcp", service)
    var resp Response
    var req = Request{Request_payload :string(ouput) , Request_emailid :string(m["email"].(string))}


         if err1 != nil {
          log.Fatal("Dialing Error:", err1)
        }
        //req:=Request{Request_payload :`{ "email": "foo1@gmail.com", "zip": "94112", "country": "U.S.A", "profession": "student", "favorite_color": "blue", "is_smoking": "yes|no", "favorite_sport": "hiking", "food": { "type": "vegetrian|meat_eater|eat_everything", "drink_alcohol": "yes|no" }, "music": { "spotify_user_id": "wizzler" }, "movie": { "tv_shows": ["x", "y", "z"], "movies": ["x", "y", "z"] }, "travel": { "flight": { "seat": "aisle|window" } } }` , Request_emailid :`foo1@gmail.com`}
        
        err := client.Call("Req.Post_Handler", req, &resp)
        fmt.Printf(resp.Response_code,resp.Response_payload)

        if err != nil {
          log.Fatal("error:", err)
        }else{

          client.Close()
        }
    
        //Check if the user Profile already exists for the given Id:
        //if not create a Hashmap for the given id
       emailId:=string(m["email"].(string))

       if (record_exists(emailId)==0){
  
             if err != nil {
                                 os.Exit(1)
                          }
                bsrec, _ := bson.Marshal(m)
                coll.SaveBson(bsrec)
                fmt.Printf("\nSaved data")

             res, _ := coll.Find(`{"email" : {"$begin" : "foo"}}`) // Name starts with 'Bru' string
                          fmt.Printf("\n\nRecords found: %d\n", len(res))
                          for _, bs := range res {
                              var m map[string]interface{}
                              bson.Unmarshal(bs, &m)
                              fmt.Println(m)
                          }
            w.WriteHeader(http.StatusCreated)
       }else{ 
              fmt.Println("Record Exists")
              w.WriteHeader(200)
  
       }
    

}



//Client GET Profile 

//Retrive the Posted Profile with Email Id as Input key 
func getprofile(w http.ResponseWriter, r *http.Request) {
      
     params := r.URL.Query()
     emailId  := params.Get(":emailId")

//Check the Presence of Record for the corresponding email id in Global hashMap 
       if record_exists(emailId)==1 {

             fmt.Println("Email Id is:",string(emailId))

             email_string:=`{"email" : "`+ string(emailId) +`"}`
             fmt.Println("Email: ",email_string)

            res, _ := coll.Find(email_string) // Name starts with 'Bru' string
                          fmt.Printf("\n\nRecords found: %d\n", len(res))
         
                        for _, bs := range res {
                              var m map[string]interface{}
                              bson.Unmarshal(bs, &m)
                              fmt.Println(m)
                              json.NewEncoder(w).Encode(m)
                          } 

                         
       }else{ 
 
       	    fmt.Println("%s %s","Profile does not Exists ",string(emailId))
   
            w.WriteHeader(http.StatusNotFound)
            fmt.Fprintf(w, "%s\n %s ","No Profile Found : the Email Id",string(emailId))

        }

 }



//Client Delete Profile 

func deleteprofile(w http.ResponseWriter, r *http.Request){

//Fetch the Email Id and check the presence of Record in the Global HashMap
     params := r.URL.Query()
     emailId  := params.Get(":emailId")

    //Handle RPC delete
    
//    var service = replica
    var client, err1 = rpc.Dial("tcp", service)
    var resp Response

   var req = Request{Request_emailid :string(emailId)}

    if err1 != nil {
          log.Fatal("Dailing Error:", err1)
        }

       fmt.Println("Entering TCP") 
    err := client.Call("Req.Delete_Handler", req, &resp)

    if err != nil {
            log.Fatal("Dailing Error:", err)
          }else{

          client.Close()
        }

       fmt.Println("Exiting TCP") 
    fmt.Printf(resp.Response_code,resp.Response_payload)

    if record_exists(emailId)==1 {

       //Execte the Delete if Record Found  
     
       email_string:=`{"email" : "`+ string(emailId) +`"}`

       res, _ := coll.Find(email_string) 
      
       for _, bs := range res {
                              var m map[string]interface{}
                              bson.Unmarshal(bs, &m)
                              res:=coll.RmBson((m["_id"].(bson.ObjectId)).Hex())
                                log.Println(res)

                           } 


       fmt.Println("%s %s","Profile Deleted",string(emailId))
       w.WriteHeader(http.StatusNoContent)
   
    }else {
     	  fmt.Println("%s %s","No Profile found to Delete :",string(emailId))
        w.WriteHeader(http.StatusNotFound)
        fmt.Fprintf(w, "%s\n %s","No Profile found to Delete :",string(emailId))

    }

}



//Client PUT Profile 

func putprofile(w http.ResponseWriter, r *http.Request){
    params := r.URL.Query()
    emailId  := params.Get(":emailId")

//   Check the Presence of Record Corresponding to the given Email Id

     if record_exists(emailId)==1 {


            var g interface{}
            json.NewDecoder(r.Body).Decode(&g)

            mg := g.(map[string]interface{})

            ouput,_:=json.Marshal(mg)

            fmt.Println("This is string",string(ouput))

          //var service = replica
          var client, err1 = rpc.Dial("tcp", service)
          var resp Response

          var req = Request{Request_payload :string(ouput),Request_emailid :string(emailId)}

          if err1 != nil {
            log.Fatal("Dailing Error:", err1)
          }

          fmt.Println("Entering TCP") 
          err := client.Call("Req.Update_Handler", req, &resp)

          if err != nil {
              log.Fatal("Dailing Error:", err)
            }else{

            client.Close()
          }

          fmt.Println("Exiting TCP") 
          fmt.Printf(resp.Response_code,resp.Response_payload)


          coll.Update(`{"email":"`+string(emailId)+`","$set":`+string(ouput)+`}`)
          

          w.WriteHeader(http.StatusNoContent)

 

              w.WriteHeader(http.StatusNoContent)

    }else{

         w.WriteHeader(http.StatusNotFound)
         fmt.Fprintf(w, "%s\n %s","Please Check Email Id No Profile Found ",string(emailId))

    }

}




func main() {
	
    //Create A new Router to Handle the Request and Handle the Requests for corresponding API's.

//Parsing TOML File 

f, err := os.Open(os.Args[1])
    if err != nil {
        panic(err)
    }
    defer f.Close()
    buf, err := ioutil.ReadAll(f)
    if err != nil {
        panic(err)
    }
    var config serverConfig
    if err := toml.Unmarshal(buf, &config); err != nil {
        panic(err)
    }
///http.ListenAndServe(":"+string(config.Database.Port_num), nil)

db_name := string(config.Database.File_name)
port_num  := strconv.Itoa(config.Database.Port_num)
rpc_server_port_num :=  strconv.Itoa(config.Replication.Rpc_server_port_num)
replica := string(config.Replication.Replica[0])

service = strings.SplitAfter(replica,"//")[1]

fmt.Println(db_name,port_num,rpc_server_port_num,replica,service)

jb, _ = goejdb.Open(db_name, goejdb.JBOWRITER | goejdb.JBOCREAT | goejdb.JBOTRUNC)
coll, _ = jb.CreateColl("contacts", nil)


fmt.Println(db_name)

	mux := routes.New()
	mux.Post("/profile", postprofile)
	mux.Get("/profile/:emailId", getprofile)
	mux.Put("/profile/:emailId",putprofile)
	mux.Del("/profile/:emailId",deleteprofile)
	http.Handle("/", mux)

//Go Routine for HTTP REST Listener

 go func(){
       http.ListenAndServe(":"+port_num, nil) 
     }()

  req := new(Req)
  rpc.Register(req)

  tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+rpc_server_port_num)
  checkError(err)
  fmt.Println("InGoroutinr")

  listener, err := net.ListenTCP("tcp", tcpAddr)
  checkError(err)

  defer listener.Close() 

//RPC Listener Main Routine

  for {
    conn, err := listener.Accept()
    if err != nil {
        continue
    }

    rpc.ServeConn(conn)
    conn.Close()
  }
  
}

func checkError(err error) {
    if err != nil {
        fmt.Println("Fatal error ", err.Error())
        os.Exit(1)
    }
}
