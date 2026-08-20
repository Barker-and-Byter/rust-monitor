use crate::System;

//function for extracting and returning important cpu statistics
pub fn get_cpu_stats(sys : &mut System) -> f64{
    sys.refresh_cpu_usage();
    let cpu_usage: f64 = sys.global_cpu_usage() as f64;
    println!("Total CPU Usage: {:.0}%", cpu_usage);
    cpu_usage
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_cpu_stats(){
        let mut sys = System::new_all();

        let usage = get_cpu_stats(&mut sys);
        assert!(usage >= 0.0, "CPU usage cannot be negative");
        assert!(usage <= 100.0, "CPU usage cannot be greater than 100%")
    }



}