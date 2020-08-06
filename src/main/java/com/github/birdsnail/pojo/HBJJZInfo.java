package com.github.birdsnail.pojo;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDate;

/**
 * @author BirdSnail
 * @date 2020/8/6
 */
@Data
@Builder
public class HBJJZInfo {
	private String name;
	private String city;
	private LocalDate blDate;
	private LocalDate validDate;
	private Integer num;
}
