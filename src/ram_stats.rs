use crate::System;

//function to extract ram information from the system
pub fn get_ram_stats(sys: &mut System, gb_conv: u64) -> (f32, f32, f32, f32) {
    sys.refresh_memory();

    let total_memory = sys.total_memory() as f32 / gb_conv as f32;
    let used_memory: f32 = sys.used_memory() as f32 / gb_conv as f32;
    let free_memory = sys.free_memory() as f32 / gb_conv as f32;
    let percentage_used = (used_memory / total_memory) * 100.0;

    println!(
        "total memory: {:.2} GBs (used memory: {:.2} GBs, free memory: {:.2} GBs, percentage_used {:.2}%)",
        total_memory, used_memory, free_memory, percentage_used
    );

    (total_memory, used_memory, free_memory, percentage_used)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_ram_stats() {
        let mut sys = System::new_all();
        let gb_conv: u64 = (1024 as u64).pow(3);

        let (total_memory, used_memory, free_memory, percentage_used) =
            get_ram_stats(&mut sys, gb_conv);
        assert!(
            used_memory <= (total_memory),
            "Used memory cannot be greater than total memory"
        );
        assert!(used_memory >= 0.0, "used memory cannot be less than 0");
        assert!(
            total_memory == used_memory + free_memory,
            "total_memory not equivalent to used memory plus free memory"
        );
        assert!(
            free_memory <= total_memory,
            "free memory cannot be greater than total memory"
        );
        assert!(
            percentage_used == (used_memory / total_memory) * 100.0,
            "RAM percentage is not equivalent to used memory divided by total memory multiplied by 100"
        );
    }
}
