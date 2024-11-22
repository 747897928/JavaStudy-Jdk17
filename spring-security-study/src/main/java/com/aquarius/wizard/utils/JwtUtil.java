package com.aquarius.wizard.utils;

import com.aquarius.wizard.common.utils.JSONUtils;
import com.aquarius.wizard.utils.entity.JwtUserInfo;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import java.util.Base64;
import java.util.Date;

/**
 * @author zhaoyijie
 * @since 2024/11/22 10:17
 */
public class JwtUtil {

    private JwtUtil() {
        throw new UnsupportedOperationException("JwtUtil class");
    }

    public static SecretKey getSecretKey(String base64EncodeSecretKey) {
        byte[] decodeKey = Base64.getDecoder().decode(base64EncodeSecretKey);
        return Keys.hmacShaKeyFor(decodeKey);
    }

    public static String createJwt(SecretKey secretKey, String id, String sub, Date iat, Date exp, String JwtUserInfo) {
        return Jwts.builder()
                .subject(sub)
                .id(id)
                .issuedAt(iat)
                .expiration(exp)
                .claim("userInfo", JwtUserInfo)
                .signWith(secretKey)
                .compact();

    }

    public static JwtUserInfo parseJwt(SecretKey secretKey, String jwt) {
        JwtParser jwtParser = Jwts.parser().verifyWith(secretKey).build();
        Jws<Claims> claimsJws = jwtParser.parseSignedClaims(jwt);
        Claims payload = claimsJws.getPayload();
        String userInfoJson = payload.get("userInfo", String.class);
        return JSONUtils.parseObject(userInfoJson, JwtUserInfo.class);
    }
}
