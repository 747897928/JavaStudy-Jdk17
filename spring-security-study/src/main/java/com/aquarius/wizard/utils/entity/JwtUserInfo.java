package com.aquarius.wizard.utils.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

/**
 * @author zhaoyijie
 * @since 2024/11/22 10:39
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JwtUserInfo {

    private String id;

    private String username;

    private List<String> roles;

    private List<String> permissions;

    private Date expiry;

}
