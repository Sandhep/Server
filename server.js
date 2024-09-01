import express from "express";
import http from "http";
import {Server} from "socket.io";
import cors from "cors";
import mqtt from "mqtt";
import bodyParser from "body-parser";
import schedule from "node-schedule";
import pg from "pg";

// Database Configurations

const db = new pg.Client({
    user: "postgres",
    host: "localhost",
    database: "pumphouse",
    password: "12345",
    port: 5432,
});

db.connect((err) => {
    if (err) {
        console.error('Connection error', err.stack);
    } else {
        console.log('Connected to PostgreSQL');
    }
});

function updatedb(query){
    db.query(query,(err, res) => {
        if (err) {
            console.error("Error executing update query", err.stack);
        }
    });
}

async function readdb(query){
    try{
        const res = await db.query(query);
        let output=res.rows;
        return output;
    }catch(err){
        console.error("Error executing Select query", err.stack);
    }
}

async function fetchdata(socket){
    try{
    const plantdata = await readdb("SELECT property,p_value FROM plantdetails");
    const timerdata = await readdb("SELECT session_name,session_time FROM timerdata");
    const sensordata = await readdb("SELECT sensor_name,sensor_data FROM sensordata");
    const data = { 
        Pump_State:plantdata.find(data => data.property === 'Pump_state').p_value,
        device_status:plantdata.find(data => data.property === 'Device_status').p_value,
        Timer:plantdata.find(data => data.property === 'Timer').p_value,
        Plant_mode:plantdata.find(data => data.property === 'Plant_mode').p_value,
        bt_state:plantdata.find(data=> data.property === 'bt_state').p_value,
        starttime:timerdata.find(data=> data.session_name === 'starttime').session_time,
        endtime:timerdata.find(data=> data.session_name === 'endtime').session_time,
        OHT_Float:sensordata.find(data=> data.sensor_name === 'OHT_Float').sensor_data,
        UGT_Float:sensordata.find(data=> data.sensor_name === 'UGT_Float').sensor_data
    }
    socket.emit("connected",data);
    }catch(err){
        console.error('Error starting socket connection', err.name);
    }
}

// MQTT Client Configuration

const options = {
    host: '8e3ddd6ba80a4e3e99739281bebb36d8.s1.eu.hivemq.cloud',
    port: 8883,
    protocol: 'mqtts',
    username: 'Sandhep',
    password: 'Sandhep13',
    qos:1 
}

const client = mqtt.connect(options);

client.on('connect', function () {
    console.log('Connected to the MQTT Broker');
    client.subscribe('Pumphouse/LWT/Status');
    client.subscribe('Pumphouse/Sensor');
    client.subscribe('Pumphouse/Status');
    client.subscribe('Pumphouse/ManualSwitch/Pump_State');

    fetchtimerdata();

    io.on("connection",(socket)=>{
        
        fetchdata(socket);
        
        socket.on("PumpState",(data)=>{
            pumpcontrol(data);
        });
    
        socket.on("Mode",(data)=>{
            console.log("Received Mode:",data);
            socket.broadcast.emit("Received_Mode",data);
            client.publish('Pumphouse/Mode',data.Plant_mode,{ retain: true });
            const query =`UPDATE plantdetails SET p_value ='${data.Plant_mode}' WHERE property = 'Plant_mode'`;
            updatedb(query);
        });
        
        socket.on("Timermode",(data)=>{
            console.log("Received Mode:",data);
            socket.broadcast.emit("Received_Timermode",data);
            const query =`UPDATE plantdetails SET p_value ='${data.Timer}' WHERE property = 'Timer'`;
            updatedb(query);
        });
    
        socket.on("Timerdata",(data)=>{
            console.log("Timer Data",data);
            socket.broadcast.emit("Received_Timerdata",data);
            const query1 = `UPDATE timerdata SET session_time = '${data.starttime}' WHERE session_name = 'starttime'`;
            updatedb(query1);
            const query2 = `UPDATE timerdata SET session_time = '${data.endtime}' WHERE session_name = 'endtime'`;
            updatedb(query2);
            const query3 = `UPDATE plantdetails SET p_value ='${data.bt_state}' WHERE property = 'bt_state'`;
            updatedb(query3);
            timer(data.starttime,data.endtime);
        })
    })
    
});

client.on('error', function (error) {
    console.log(error.message);
});

client.on('message', function (topic, message) {
    if (topic === "Pumphouse/LWT/Status") {
        io.emit("Device_status",{device_status:message.toString()});
        const query = `UPDATE plantdetails SET p_value ='${message.toString()}' WHERE property = 'Device_status'`;
        updatedb(query);
        
    }
    if(topic === "Pumphouse/ManualSwitch/Pump_State"){
        const ManualSwitch_State = message.toString();
        io.emit("Received_PumpState",{Pump_State:ManualSwitch_State});
        const query =`UPDATE plantdetails SET p_value ='${ManualSwitch_State}' WHERE property = 'Pump_state'`;
        updatedb(query);
    }
    if(topic === "Pumphouse/Status"){
        io.emit("Device_status",{device_status:message.toString()});
        const query = `UPDATE plantdetails SET p_value ='${message.toString()}' WHERE property = 'Device_status'`;
        updatedb(query);
      
    }
    if(topic === "Pumphouse/Sensor"){
        const sensor_data = JSON.parse(message);
        console.log("Sensor data:",sensor_data);
        io.emit("Sensordata",sensor_data);
        const query1 = `UPDATE sensordata SET sensor_data = '${sensor_data.OHT_Float}' WHERE sensor_name = 'OHT_Float'`;
        updatedb(query1);
        const query2 = `UPDATE sensordata SET sensor_data = '${sensor_data.UGT_Float}' WHERE sensor_name = 'UGT_Float'`;
        updatedb(query2);
    }
});

// Socket and Express Server

const app = express();
const Port=4000;

app.use(cors());
app.use(bodyParser.urlencoded({extended:true}));

const server = http.createServer(app);

const io = new Server(server,{
    cors:{
        origin:"http://localhost:3000"
    } 
})

server.listen(Port,()=>{
    console.log(`Server is Running on Port:${Port}`);
})

async function fetchtimerdata(){
    const timerdata = await readdb("SELECT session_name,session_time FROM timerdata");
    const data = {
        starttime:timerdata.find(data=> data.session_name === 'starttime').session_time,
        endtime:timerdata.find(data=> data.session_name === 'endtime').session_time
    }
    timer(data.starttime,data.endtime);
}

async function timer(starttime,endtime){
    let [starthour,startminute] = starttime.split(":");
    let [endHour, endMinute] = endtime.split(':');
    const startjob = schedule.scheduleJob({ hour: starthour, minute: startminute, tz: 'Asia/Kolkata' }, async function() {
           const plantdata = await readdb("SELECT property,p_value FROM plantdetails");
           const Timer = plantdata.find(data => data.property === 'Timer').p_value;
           console.log("CRON Job Started");
           if(Timer === "ENABLED"){
                pumpcontrol({Pump_State:"ON"});
           }
     });
    
    const stopjob = schedule.scheduleJob({ hour: endHour, minute: endMinute, tz: 'Asia/Kolkata' }, async function() {
           const plantdata = await readdb("SELECT property,p_value FROM plantdetails");
           const Timer = plantdata.find(data => data.property === 'Timer').p_value;
           console.log("CRON Job Stopped");
           if(Timer === "ENABLED"){
               pumpcontrol({Pump_State:"OFF"});
           }
     });
}

function pumpcontrol(data){
    console.log("Received Pump_State:",data);
    io.emit("Received_PumpState",data);
    client.publish('Pumphouse/Switch/Pump_State',data.Pump_State,{ retain: true });
    const query =`UPDATE plantdetails SET p_value ='${data.Pump_State}' WHERE property = 'Pump_state'`;
    updatedb(query);
}
