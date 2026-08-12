
use core::time;
use std::{thread::{self, current}, time::Duration};
use axum::{
    response::sse::{Event, Sse},
    routing::get,
    Router
};
use futures_util::stream::{self, Stream};
use futures_util::future::FutureExt;
use tokio;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
//import sysinfo package (yipee)
use sysinfo::{
    Components, Disks, Networks, Signal::Sys, System};
use tower_http::cors::{Any, CorsLayer};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use bollard::Docker;
use bollard::query_parameters::ListImagesOptionsBuilder;
use std::env;

//struct for network stats
#[derive(Debug, Clone)]
pub struct NetworkStats{
    pub interface: String,
    pub received: f64,
    pub transmitted: f64
}

#[derive(Debug, Clone)]
pub struct DriveStats{
    pub written_bytes: f64,
    pub read_bytes: f64,
}



//function for extracting and returning important cpu statistics
fn cpu_stats(sys : &mut System) -> f64{
    sys.refresh_cpu_usage();
    let cpu_usage: f64 = sys.global_cpu_usage() as f64;
    println!("Total CPU Usage: {:.0}%", cpu_usage);
    (cpu_usage)
}


//function to extract ram information from the system
fn ram_stats(sys: &mut System, gb_conv :u64) -> (f64, f64, f64){
    sys.refresh_memory();
    let total_memory = sys.total_memory() as f64 / gb_conv as f64 ;
    let used_memory: f64 = sys.used_memory() as f64 / gb_conv as f64;
    let free_memory = sys.free_memory() as f64/ gb_conv as f64;
    let percentage_used = (used_memory / total_memory) * 100.0;
    println!("total memory: {:.2} GBs (used memory: {:.2} GBs, free memory: {:.2} GBs, percentage_used {:.2}%)", 
    total_memory,
    used_memory,
    free_memory,
    percentage_used) ;

    (used_memory, free_memory, percentage_used)


}

//function for extracting disk metrics
fn disk_usage(sys : &mut System,gb_conv :u64) -> (f64, f64, f64, f64){
    let disks = Disks::new_with_refreshed_list();
    let mut total_space: f64 = 0.00;
    let mut available_space: f64 = 0.00;
    let mut used_space: f64 = 0.00;
    let mut disk_percentage_used: f64 = 0.00;

    for disk in disks.list() {
        println!("[{:?}] disk total space: {:.2} GBs, (disk available space: {:.2} GBs, disk used space {:.0} GBs, )",
        disk.name(),
        disk.total_space() as f64 / gb_conv as f64,
        disk.available_space() as f64 / gb_conv as f64,
        (disk.total_space() as f64 - disk.available_space() as f64) / gb_conv as f64,

        );
        total_space += disk.total_space() as f64;
        available_space += disk.available_space() as f64;
        used_space += (disk.total_space() as f64 - disk.available_space() as f64);
        disk_percentage_used += (used_space / total_space * 100.00);
    }

    (total_space, available_space, used_space, disk_percentage_used)

}

async fn start_drive_monitor(tx: mpsc::Sender<Vec<DriveStats>>){
    let mut disks = Disks::new_with_refreshed_list();
    loop{
        std::thread::sleep(Duration::from_secs(1));
        disks.refresh(true);
        let mut disk_stats: Vec<_> = Vec::new();
        for disk in &disks{
            println!("{disk:?}");
            disk_stats.push(DriveStats {
                written_bytes: disk.usage().written_bytes as f64,
                read_bytes: disk.usage().read_bytes as f64
            });
        }
        if tx.send(disk_stats).await.is_err(){
            break;
        }


    }
}


async fn start_network_monitor(tx: mpsc::Sender<Vec<NetworkStats>>){
    let mut networks = Networks::new_with_refreshed_list();
        loop {
        std::thread::sleep(Duration::from_secs(1));
        networks.refresh(true);
        let mut current_stats = Vec::new();
        for (interface_name, data) in &networks{
            current_stats.push(NetworkStats {
                interface: interface_name.clone(),
                received: data.received() as f64,
                transmitted: data.transmitted() as f64

            });
        }
        if tx.send(current_stats).await.is_err(){
            break;
        }

    }
}



#[tokio::main]
async fn main(){
    let port = env::var("PORT")
    .unwrap_or_else(|_| "3000".to_string());

    let cors = CorsLayer::new()
    .allow_origin(Any)
    .allow_methods([http::Method::GET, http::Method::POST, http::Method::OPTIONS])
    .allow_headers([CONTENT_TYPE, AUTHORIZATION]);

    let app = Router::new().route("/cpu-stream", get(sse_handler)).layer(cors);

    let addr = format!("0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("server now listening on http://{}", addr);
    axum::serve(listener, app).await.unwrap();

}

async fn sse_handler () -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>{
    //create a channel to receive the information with a buffer of 10 
    let (net_tx, mut net_rx) = mpsc::channel::<Vec<NetworkStats>>(10);
    let (drive_tx, mut drive_rx) = mpsc::channel::<Vec<DriveStats>>(10);
    tokio::spawn(start_network_monitor(net_tx));
    tokio::spawn(start_drive_monitor(drive_tx));
    let x: u64 = 1024;
    let gb_conv = x.pow(3);

    let mut sys = System::new_all();
    let stream = stream:: repeat_with(move || {
        println!("=> Cpu information");
        let cpu_usage: f64 = cpu_stats(&mut sys);
        println!("=> Ram information");
        let (used_memory, free_memory, ram_percentage_used) = ram_stats(&mut sys, gb_conv);
        println!("=> disk information");
        let (total_space, available_space, used_space, disk_percentage_used) = disk_usage(&mut sys, gb_conv);
        let disk_stats:Vec<DriveStats> = match drive_rx.try_recv(){
            Ok(dstats) => dstats,
            Err(_) => Vec::new()
        };
        println!("=> network information");
        let network_stats = match net_rx.try_recv(){
            Ok(stats) => stats,
            Err(_) => Vec::new()
        };
        let mut total_transmitted = 0.0;
        let mut total_received = 0.0;
        for stats in &network_stats {
            total_transmitted += stats.transmitted;
            total_received += stats.received;
        }
        let mut total_written: f64 = 0.0;
        let mut total_read: f64 = 0.0;
        for dstats in &disk_stats{
            total_written += dstats.written_bytes;
            total_read += dstats.read_bytes;

        }
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let json_payload = format!(
            r#"{{"timestamp":{},"cpuUsage":{:.0},"ramUsage":{:.0},"ramUsed":{:.2},"ramFree":{:.2},"driveUsage":{:.2},"driveUsed":{:.2},"driveFree":{:.2},"uploadSpeed":{:.2},"downSpeed":{:.2},"written":{:.2},"read":{:.2}}}"#,
            timestamp,
            cpu_usage,
            ram_percentage_used,
            used_memory,
            free_memory,
            disk_percentage_used,
            used_space /1024.0/ 1024.0/ 1024.0,
            available_space / 1024.0/ 1024.0/ 1024.0,
            total_transmitted / 1024 as f64,
            total_received / 1024 as f64,
            total_written / 1024 as f64,
            total_read / 1024 as f64

        );

        Event::default().data(json_payload)
    })
    .throttle(std::time::Duration::from_millis(1000))
    .map(Ok);

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
        .interval(std::time::Duration::from_secs(15))
    )
}
