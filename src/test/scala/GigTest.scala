import org.openspaces.core.space.UrlSpaceConfigurer
import org.openspaces.core.GigaSpace
import org.openspaces.core.GigaSpaceConfigurer
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
import com.gigaspaces.document.SpaceDocument
import org.simeont.batch.ProcecessedOperationActionHolder

object GigTest extends App {

    val urlSpaceConfigurer: UrlSpaceConfigurer = new UrlSpaceConfigurer("jini://*/*/dataSourceSpace")
      .lookupGroups("gigaspaces-9.5.0-XAPPremium-ga")

    val gigaSpace: GigaSpace = new GigaSpaceConfigurer(urlSpaceConfigurer).gigaSpace()

    val spDesc = new SpaceTypeDescriptorBuilder("test").idProperty("id").create()

    gigaSpace.getTypeManager().registerTypeDescriptor(spDesc)

    val doc = new SpaceDocument("test")

    doc.setProperty("id", "test1")
    doc.setProperty("log", 1)
    gigaSpace.write(doc)

    //  val permissions = Array(
//    ContentPermission.newInsertPermission("admin"),
//    ContentPermission.newReadPermission("admin"),
//    ContentPermission.newUpdatePermission("admin"))
//  val tempContentCreateOptions = new ContentCreateOptions()
//  tempContentCreateOptions.setNamespace("tomo")
//  tempContentCreateOptions.setPermissions(permissions)
//  tempContentCreateOptions.setCollections(Array("tomo"))
//
//  val content = ContentFactory.newContent("/test/test4.xml", 
//      "<test xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" ><id xs:type=\"java.lang.String\">test3</id></test>", 
//      tempContentCreateOptions)
//      
//  val contentSource = ContentSourceFactory.newContentSource("localhost", 8022, "admin", "admin")
//  
//  val session = contentSource.newSession()
//
//  session.insertContent(content)
}
