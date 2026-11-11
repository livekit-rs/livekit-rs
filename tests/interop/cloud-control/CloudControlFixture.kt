@file:OptIn(ExperimentalForeignApi::class)

package msgtrans.transport

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.toKString
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.io.net.runReactor
import neton.openssl.TlsContext
import neton.tls.tlsAccept
import platform.posix.fclose
import platform.posix.fopen
import platform.posix.fread
import platform.posix.getenv
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds

class CloudControlFixture {
    private fun env(name: String): String? = getenv(name)?.toKString()?.takeIf { it.isNotEmpty() }

    private fun readFile(path: String): ByteArray {
        val f = fopen(path, "rb") ?: error("cannot open $path")
        val out = ArrayList<Byte>()
        try {
            memScoped {
                val buf = allocArray<ByteVar>(4096)
                while (true) {
                    val n = fread(buf, 1u, 4096u, f).toInt()
                    if (n <= 0) break
                    out.addAll(buf.readBytes(n).toList())
                }
            }
        } finally {
            fclose(f)
        }
        return out.toByteArray()
    }

    @Test
    fun rustControl() = runReactor {
        val port = env("LK_CLOUD_FIXTURE_PORT")?.toInt() ?: error("missing fixture port")
        val tls = TlsContext(
            server = true,
            certificateChainPem = readFile(env("LK_CLOUD_FIXTURE_CERT")!!),
            privateKeyPem = readFile(env("LK_CLOUD_FIXTURE_KEY")!!),
            alpnProtocols = listOf("http/1.1"),
        )
        val transport = WebSocketServerTransport("127.0.0.1", port, tlsAcceptor = { tlsAccept(it, tls) })
        val done = CompletableDeferred<Unit>()
        val expected = listOf(1, 3, 4, 2, 3, 4, 2, 1, 3, 4, 2)
        val start = platform.posix.time(null) * 1000
        var received = 0
        val server = Transport.bind(this, transport, ConnectionConfig(requiredFirstRequestBizType = 1)) { conn ->
            conn.onClose { if (received == expected.size) done.complete(Unit) }
            conn.onRequest { bytes, biz ->
                check(received < expected.size && biz == expected[received])
                val request = bytes.decodeToString()
                val step = received++
                val data = when (biz) {
                    1 -> {
                        check(request.contains("\"node_id\":\"node-a\""))
                        check(request.contains("\"credential\":\"fixture-only\""))
                        check(request.contains("\"server_epoch\":1"))
                        val generation = if (step == 0) 1 else 2
                        """{"generation":$generation,"config_version":42,"heartbeat_interval_ms":1000}"""
                    }
                    3 -> {
                        check(request.contains(if (step == 1) "\"since\":null" else "\"since\":42"))
                        val issued = start + step
                        """{"contract_version":1,"region":"test-1","config_version":42,"snapshot":true,"issued_at":$issued,"valid_until":${issued + 600000},"projects":[]}"""
                    }
                    4 -> {
                        check(request.contains("\"applied_version\":42") && request.contains("\"errors\":[]"))
                        "true"
                    }
                    2 -> {
                        check(request.contains("\"state\":\"STARTING\""))
                        check(request.contains("\"applied_version\":42"))
                        if (step == 3) conn.send("""{"config_version":42}""".encodeToByteArray(), bizType = 16)
                        if (step == 6 || step == 10) conn.launch {
                            kotlinx.coroutines.delay(100)
                            conn.close()
                        }
                        """{"config_version":42,"server_time_ms":$start}"""
                    }
                    else -> error("unexpected control request")
                }
                """{"code":0,"message":"ok","data":$data}""".encodeToByteArray()
            }
        }
        val accepting = launch { server.acceptLoop() }
        println("SPEC0078 fixture listening")
        try {
            withTimeout(15.seconds) { done.await() }
            check(received == expected.size)
            println("SPEC0078 fixture verified ${expected.joinToString(",")}")
        } finally {
            accepting.cancelAndJoin()
            server.close()
            tls.close()
        }
    }
}
