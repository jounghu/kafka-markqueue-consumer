package com.dobest.kafka.processor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 2020/4/20 14:51
 *
 * @author hujiansong@dobest.com
 * @since 1.8
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {
    String username;
    String password;
}
