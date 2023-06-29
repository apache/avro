package custom;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import com.github.davidmc24.gradle.plugin.avro.AvroPlugin;
import com.github.davidmc24.gradle.plugin.avro.AvroExtension;

public class AvroConventionPlugin implements Plugin<Project> {
    public void apply(Project project) {
        project.getPluginManager().apply(AvroPlugin.class);
        AvroExtension avroExtension = project.getExtensions().findByType(AvroExtension.class);
        avroExtension.logicalTypeFactory("timezone", TimeZoneLogicalTypeFactory.class);
        avroExtension.customConversion(TimeZoneConversion.class);
    }
}
