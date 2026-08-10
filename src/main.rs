
use core::time;
use axum::{
    response::sse::{Event, Sse},
    routing::get,
    Router
};
use futures_util::stream::{self, Stream};
use tokio;
use tokio_stream::StreamExt;
//import sysinfo package (yipee)
use sysinfo::{
    Components, Disks, Networks, Signal::Sys, System};
use tower_http::cors::{Any, CorsLayer};
use http::header::{AUTHORIZATION, CONTENT_TYPE};


//struct for network stats




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
    let mut disks = Disks::new_with_refreshed_list();
    let mut total_space: f64 = 0.00;
    let mut available_space: f64 = 0.00;
    let mut used_space: f64 = 0.00;
    let mut disk_percentage_used: f64 = 0.00;
    std::thread::sleep(time::Duration::from_millis(10));
    disks.refresh(true);

    for disk in disks.list() {
        println!("[{:?}] disk total space: {:.2} GBs, (disk available space: {:.2} GBs, disk used space {:.0} GBs, Written GBS {:?} )",
        disk.name(),
        disk.total_space() as f64 / gb_conv as f64,
        disk.available_space() as f64 / gb_conv as f64,
        (disk.total_space() as f64 - disk.available_space() as f64) / gb_conv as f64,
        disk.usage(),
        );
        total_space += disk.total_space() as f64;
        available_space += disk.available_space() as f64;
        used_space += (disk.total_space() as f64 - disk.available_space() as f64);
        disk_percentage_used += (used_space / total_space * 100.00);
    }

    (total_space, available_space, used_space, disk_percentage_used)

}

fn network_stats() -> (f64, f64) {
    let mut networks = Networks::new_with_refreshed_list();


    tokio::time::sleep(Duration::from_millis(10));
    networks.refresh(true);
    let mut received: f64 = 0.0;
    let mut transmitted: f64 = 0.0;
    for (interface_name, network) in &networks{
        received += network.received() as f64;
        transmitted += network.transmitted() as f64;
        println!("received: {} Bytes", network.received());
        println!("transmitted: {} Bytes", network.transmitted());
    }
    (received, transmitted)
    
}


async fn start_network_monitor(tx: tokio::mspc::Sender<Vec<NetworkStats>>){

}


#[tokio::main]
async fn main(){
    let cors = CorsLayer::new()
    .allow_origin(Any)
    .allow_methods([http::Method::GET, http::Method::POST, http::Method::OPTIONS])
    .allow_headers([CONTENT_TYPE, AUTHORIZATION]);

    let app = Router::new().route("/cpu-stream", get(sse_handler)).layer(cors);

    let listener = tokio::net::TcpListener::bind("localhost:3000").await.unwrap();
    println!("server now listening on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();

}

async fn sse_handler () -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>{
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
        println!("=> network information");
        let (received, transmitted) = network_stats();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let json_payload = format!(
            r#"{{"timestamp":{},"cpuUsage":{:.0},"ramUsage":{:.0},"ramUsed":{:.2},"ramFree":{:.2},"driveUsage":{:.2},"driveUsed":{:.2},"driveFree":{:.2},"uploadSpeed":{:.2},"downSpeed":{:.2}}}"#,
            timestamp,
            cpu_usage,
            ram_percentage_used,
            used_memory,
            free_memory,
            disk_percentage_used,
            used_space /1024.0/ 1024.0/ 1024.0,
            available_space / 1024.0/ 1024.0/ 1024.0,
            transmitted / 1024 as f64,
            received / 1024 as f64
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
