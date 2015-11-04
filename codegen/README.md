Avro code generation Plugin
===========================

This AutoPlugin will generate case classes to marshal/unmarshal the avro messages that correspond to the avro schemas present in the source folder for that effect.

To use this plugin you will need to add it to the list of plugins. In project/plugins.sbt:

```addSbtPlugin("com.nitro.platform" % "avro-codegen-compiler" % "0.0.1-SNAPSHOT")```

The avro schemas must be present in the folder

```src/main/avro```

This setting can be overriden with:

```sourceDirectory in avro := "/my/custom/folder"```

Yo will need to add the required dependencies:

```
  "com.nitro.platform" %% "avro-codegen-runtime" % "0.0.1-SNAPSHOT"
```

Limitations
----
* IntelliJ compilation does not run the generation 
* Generated sources folder is not added to the managedSources folder. The default location is below the managedSources folder and thus included in the compilation, but if the location is changed and not in a managedSources folder, the developer needs to add this custom folder to the managedSources ``` managedSourceDirectories in Compile += baseDirectory.value / "my" / "custom" / "folder" ```