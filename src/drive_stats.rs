use crate::Disks;
use crate::System;

//function for extracting disk metrics
pub fn disk_usage(_sys: &mut System, gb_conv: u64) -> (f32, f32, f32, f32) {
    let disks = Disks::new_with_refreshed_list();
    let mut total_space: f32 = 0.00;
    let mut available_space: f32 = 0.00;
    let mut used_space: f32 = 0.00;
    let mut disk_percentage_used: f32 = 0.00;

    for disk in disks.list() {
        println!(
            "[{:?}] disk total space: {:.2} GBs, (disk available space: {:.2} GBs, disk used space {:.0} GBs, )",
            disk.name(),
            disk.total_space() as f32 / gb_conv as f32,
            disk.available_space() as f32 / gb_conv as f32,
            (disk.total_space() as f32 - disk.available_space() as f32) / gb_conv as f32,
        );
        total_space += disk.total_space() as f32;
        available_space += disk.available_space() as f32;
        used_space += disk.total_space() as f32 - disk.available_space() as f32;
        disk_percentage_used += used_space / total_space * 100.00;
    }

    (
        total_space,
        available_space,
        used_space,
        disk_percentage_used,
    )
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_drive_stats() {
        let mut sys = System::new_all();
        let gb_conv = (1024 as u64).pow(3);

        let (total_space, available_space, used_space, disk_percentage_used) =
            disk_usage(&mut sys, gb_conv);
        assert!(
            available_space <= total_space,
            "Available disk space cannot be greater than the total space"
        );
        assert!(
            total_space == available_space + used_space,
            "Total space must be equal to available space + used space"
        );
        assert!(
            disk_percentage_used == (used_space / total_space) * 100.0,
            "Disk percentage is not equivalent to used space divided by total space multiplied by 100"
        );
        assert!(
            used_space <= total_space,
            "Used space cannot be greater than total space"
        );
    }
}
