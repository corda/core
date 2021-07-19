package net.corda.webserver.servlets

import net.corda.core.internal.extractFile
import net.corda.core.crypto.SecureHash
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.utilities.contextLogger
import java.io.FileNotFoundException
import java.io.IOException
import java.util.jar.JarInputStream
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.core.MediaType

/**
 * Allows the node administrator to either download full attachment zips, or individual files within those zips.
 *
 * GET /attachments/123abcdef12121            -> download the zip identified by this hash
 * GET /attachments/123abcdef12121/foo.txt    -> download that file specifically
 *
 * Files are always forced to be downloads, they may not be embedded into web pages for security reasons.
 *
 * TODO: See if there's a way to prevent access by JavaScript.
 * TODO: Provide an endpoint that exposes attachment file listings, to make attachments browsable.
 */
class AttachmentDownloadServlet : HttpServlet() {
    companion object {
        private val log = contextLogger()
    }

    @Throws(IOException::class)
    override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
        val reqPath = req.pathInfo?.substring(1)
        if (reqPath == null) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST)
            return
        }

        try {
            val hash = SecureHash.create(reqPath.substringBefore('/'))
            val rpc = servletContext.getAttribute("rpc") as CordaRPCOps
            val attachment = rpc.openAttachment(hash)

            // Don't allow case sensitive matches inside the jar, it'd just be confusing.
            val subPath = reqPath.substringAfter('/', missingDelimiterValue = "").toLowerCase()

            resp.contentType = MediaType.APPLICATION_OCTET_STREAM
            if (subPath.isEmpty()) {
                resp.addHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"$hash.zip\"")
                attachment.use { it.copyTo(resp.outputStream) }
            } else {
                val filename = subPath.split('/').last()
                resp.addHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"$filename\"")
                JarInputStream(attachment).use { it.extractFile(subPath, resp.outputStream) }
            }

            // Closing the output stream commits our response. We cannot change the status code after this.
            resp.outputStream.close()
        } catch (e: FileNotFoundException) {
            log.warn("404 Not Found whilst trying to handle attachment download request for ${servletContext.contextPath}/$reqPath")
            resp.sendError(HttpServletResponse.SC_NOT_FOUND)
            return
        }
    }
}
