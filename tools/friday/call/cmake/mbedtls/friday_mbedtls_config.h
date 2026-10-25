/* Friday：Mbed TLS 的追加配置（MBEDTLS_USER_CONFIG_FILE，cmake/FridayCallDeps.cmake）。
 * libdatachannel 的 DTLS-SRTP（WebRTC 的媒体加密协商）要它，mbedtls_config.h 默认没开。 */
#define MBEDTLS_SSL_DTLS_SRTP
