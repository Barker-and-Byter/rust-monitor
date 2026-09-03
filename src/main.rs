//dependencies
use axum::{
    Router,
    response::sse::{Event, Sse},
    routing::{any, get, post},
    http::StatusCode,
    {Json, response::IntoResponse}
};
use time::SignedDuration;
use std::{
    collections::HashSet,
    sync::{Mutex, OnceLock},
};
use rand::RngExt;
use serde::Deserialize;
use axum_extra::extract::{CookieJar, cookie::Cookie};
use docker_wrapper::command::compose::start;
pub use docker_wrapper::{DockerCommand, PsCommand, StatsCommand};
use futures_util::{
    future::err,
    stream::{self, Stream},
};
use std::{env, io::{self, Write}};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use std::{thread::sleep, time::Duration};
pub use sysinfo::{Disks, Networks, System};
use dotenv::dotenv;
use tokio;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::IntervalStream};
use tower_http::cors::{AllowCredentials, AllowOrigin, Any, CorsLayer};

//modules
mod cpu_stats;
mod drive_stats;
mod ram_stats;

//struct for network stats
#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub interface: String,
    pub received: f64,
    pub transmitted: f64,
}

// struct for drive stats
#[derive(Debug, Clone)]
pub struct DriveStats {
    pub written_bytes: f64,
    pub read_bytes: f64,
}

//struct for authentication
#[derive(Deserialize)]
struct AuthRequest {
    secret: String,
}

//sessions storage for front end server 
static SESSIONS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn sessions() -> &'static Mutex<HashSet<String>> {
    SESSIONS.get_or_init(|| Mutex::new(HashSet::new()))
}

// struct for docker stats
#[derive(Debug, Clone)]
pub struct DockerStats {
    pub name: String,
    pub dock_cpu_perc: f64,
    pub dock_ram_perc: f64,
    pub dock_net_io: String,
    pub dock_block_io: String,
}
fn clear_console() {
    print!("\x1B[2J\x1B[1;1H");
    
    // Flush stdout to guarantee the terminal updates immediately
    std::io::stdout().flush().unwrap();
}

async fn start_docker_monitor(tx: mpsc::Sender<Vec<DockerStats>>) {
    if std::process::Command::new("docker").arg("--version").output().is_err() {
        eprintln!("Docker is not installed or not in PATH. Stopping Docker monitor.");
        return;
    }

    let stats_command = StatsCommand::new().format("json").no_stream();
    loop {
        let mut docker_stats = Vec::new();
        let stats_result = stats_command.run().await;
        match stats_result {
            Ok(stats) => {
                if stats.success() {
                    for stat in &stats.parsed_stats {
                        println!(
                            "Container name: {:?} CPU_usage percentage: {:?} Memory Usage {:?} Network I/O {:?} Block I/O {:?} ",
                            stat.name,
                            stat.cpu_percentage().unwrap_or(0.0),
                            stat.memory_percentage().unwrap_or(0.0),
                            stat.network_io,
                            stat.block_io
                        );
                        docker_stats.push(DockerStats {
                            name: stat.name.clone(),
                            dock_cpu_perc: stat.cpu_percentage().unwrap_or(0.0),
                            dock_ram_perc: stat.memory_percentage().unwrap_or(0.0),
                            dock_net_io: stat.network_io.clone(),
                            dock_block_io: stat.block_io.clone(),
                        });
                    }
                    if let Err(e) = tx.send(docker_stats).await {
                        println!("error in sending docker stats to main function {}", e);
                        break;
                    }
                } else {
                    eprintln!(
                        "Unnsucessful command for docker stats Error : {:?}",
                        stats.output
                    );
                }
            }
            Err(err) => {
                eprintln!("Error collecting stats! error is {:?}", err);
            }
        }
        sleep(Duration::from_secs(1));
    }
}

async fn start_drive_monitor(tx: mpsc::Sender<Vec<DriveStats>>) {
    let mut disks = Disks::new_with_refreshed_list();
    loop {
        std::thread::sleep(Duration::from_secs(1));
        disks.refresh(true);
        let mut disk_stats: Vec<_> = Vec::new();
        for disk in &disks {
            println!("{disk:?}");
            disk_stats.push(DriveStats {
                written_bytes: disk.usage().written_bytes as f64,
                read_bytes: disk.usage().read_bytes as f64,
            });
        }
        if tx.send(disk_stats).await.is_err() {
            break;
        }
    }
}

async fn start_network_monitor(tx: mpsc::Sender<Vec<NetworkStats>>) {
    let mut networks = Networks::new_with_refreshed_list();
    loop {
        std::thread::sleep(Duration::from_secs(1));
        networks.refresh(true);
        let mut current_stats = Vec::new();
        for (interface_name, data) in &networks {
            current_stats.push(NetworkStats {
                interface: interface_name.clone(),
                received: data.received() as f64,
                transmitted: data.transmitted() as f64,
            });
        }
        if tx.send(current_stats).await.is_err() {
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    let ports = vec![80, 3001, 3002];
    let mut actual_addr = None;
    let mut listener = None;
    // let mut port_index =0;
    // let port = env::var("PORT")
    // .unwrap_or_else(|_| ports[port_index].to_string());

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::exact("http://localhost:5173".parse().unwrap()))
        .allow_methods([http::Method::GET, http::Method::POST, http::Method::OPTIONS])
        .allow_headers([CONTENT_TYPE, AUTHORIZATION])
        .allow_credentials(true);

    let app = Router::new()
        .route("/data-stream", get(sse_handler))
        .route("/api/authenticate", post(authenticate_handler))
        .layer(cors);

    for port in ports {
        let addr = format!("0.0.0.0:{}", port);

        match tokio::net::TcpListener::bind(&addr).await {
            Ok(res) => {
                actual_addr = Some(addr);
                listener = Some(res);
                break;
            }
            Err(e) => {
                println!(
                    "Unsuccesful connection on port {} the error was {}, \ntrying next port",
                    port, e
                );
            }
        }
    }
    let listener = match listener {
        Some(l) => l,
        None => panic!("We no connect to any ports!!!!!"),
    };
    println!(
        "server is now listening on http://{:?}",
        actual_addr.unwrap()
    );
    axum::serve(listener, app).await.unwrap();
}


async fn authenticate_handler (
    jar: CookieJar,
    Json(payload): Json<AuthRequest>,
) -> impl IntoResponse {
    dotenv().ok();
    let expected_secret = env::var("PRIVATE_API_TOKEN").unwrap_or_default();

    if payload.secret != expected_secret {
        return (StatusCode::UNAUTHORIZED, jar).into_response();
    }

    let session_token: String = rand::rng()
    .sample_iter(&rand::distr::Alphanumeric)
    .take(32)
    .map(char::from)
    .collect();

    sessions().lock().unwrap().insert(session_token.clone());

    let cookie = Cookie::build(("auth_token", session_token))
        .path("/")
        .http_only(true)
        .same_site(axum_extra::extract::cookie::SameSite::Lax)
        .secure(false)
        .max_age(SignedDuration::hours(12))
        .build();
   

    (StatusCode::OK, jar.add(cookie)).into_response()
}


async fn sse_handler(
    jar: CookieJar,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, StatusCode> {


    let authorised =  jar
    .get("auth_token")
    .map(|cookie| cookie.value().to_string())
    .map(|token| sessions().lock().unwrap().contains(&token))
    .unwrap_or(false);

    if !authorised {
        return Err(StatusCode::UNAUTHORIZED);
    }


    //create a channel to receive the information with a buffer of 5
    let (docker_tx, mut docker_rx) = mpsc::channel::<Vec<DockerStats>>(5);
    let (net_tx, mut net_rx) = mpsc::channel::<Vec<NetworkStats>>(5);
    let (drive_tx, mut drive_rx) = mpsc::channel::<Vec<DriveStats>>(5);

    tokio::spawn(start_network_monitor(net_tx));
    tokio::spawn(start_drive_monitor(drive_tx));
    tokio::spawn(start_docker_monitor(docker_tx));

    let x: u64 = 1024;
    let gb_conv = x.pow(3);
    let mut sys = System::new_all();
    let mut last_docker_stats: Vec<DockerStats> = Vec::new();
    let interval = tokio::time::interval(Duration::from_secs(1));


    let stream = IntervalStream::new(interval).map(move |_| {
        // clear_console();
        let hostname = System::host_name().unwrap_or_default();

        println!("=>hostname! {}",
        hostname
        );


        println!("=> Cpu information");
        let cpu_usage: f32 = cpu_stats::get_cpu_stats(&mut sys);

        println!("=> Ram information");
        let (_total_memory, used_memory, free_memory, ram_percentage_used) = ram_stats::get_ram_stats(&mut sys, gb_conv);

        println!("=> disk information");
        let (_total_space, available_space, used_space, disk_percentage_used) = drive_stats::disk_usage(&mut sys, gb_conv);

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

       

        let docker_stats = match docker_rx.try_recv() {
            Ok(doc_stats) => {
                last_docker_stats = doc_stats
            }
            Err(_) => {

            }
        };
        let docker_stats =&last_docker_stats;


        let containers_json_elements: Vec<String> = docker_stats.iter().map(|doc_stat| {
            format!(
                r#"{{"name" : {:?}, "dock_cpu_perc":{:.2}, "dock_ram_perc":{:.2}, "dock_net_io": {:?}, "dock_block_io": {:?}}}"#,
                doc_stat.name,
                doc_stat.dock_cpu_perc,
                doc_stat.dock_ram_perc,
                doc_stat.dock_net_io,
                doc_stat.dock_block_io
            )
        }).collect();

        let containers_json_array = containers_json_elements.join(",");

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let json_payload = format!(
            r#"{{"timestamp":{},"cpuUsage":{:.0},"ramUsage":{:.0},"ramUsed":{:.2},"ramFree":{:.2},"driveUsage":{:.2},"driveUsed":{:.2},"driveFree":{:.2},"uploadSpeed":{:.2},"downSpeed":{:.2},"written":{:.2},"read":{:.2},"hostname":{:?},"containers":[{}]}}"#,
            timestamp,
            cpu_usage.round(),
            ram_percentage_used,
            used_memory,
            free_memory,
            disk_percentage_used,
            used_space /1024.0/ 1024.0/ 1024.0,
            available_space / 1024.0/ 1024.0/ 1024.0,
            total_transmitted / 1024 as f64,
            total_received / 1024 as f64,
            total_written / 1024 as f64,
            total_read / 1024 as f64,
            hostname,
            containers_json_array,
        );

        Ok::<Event, std::convert::Infallible>(Event::default().data(json_payload))
    });

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new().interval(std::time::Duration::from_secs(15)),
    ))
}
