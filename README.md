# DEFLOX

This repository hosts the production code of the DEFLOX project.
Further information are hosted within the (private) [BC Sharepoint](https://brockmannconsult.sharepoint.com/sites/DEFLOX).

## DEFLOX context

The project revolves around FLoX machines. FLoXes are machines that take measurements on the ground, e.g. radiances.
They are used for calibration, validation, and more, and are used in the ESA FlexDisc project, which is the validation project for the ESA FLEX mission.
ESA FLEX (FLuorescence EXplorer) will provide global maps of vegetation fluorescence that can reflect photosynthetic activity and plant health and stress.

The project's prime are JB Hyperspectral, Düsseldorf, with the key persons Tommaso Julitta and Andreas Burkart.

There are already dozens of FLoXes around the world, and within the project's runtime, there shall be > 100.

One of the goals of the project is to streamline the data collections that come from the FLoXes. Data shall be stored in a geographical database.

## Repository contents

This repository contains:
- the code that handled the translation between the FloX-internal CSV-like format and the xcube geoDB
- the control code for the systematic processing

## Ingestion

The workflow consists of
- checking the FTP for new data
  - do not keep any state but ask the database
- copying the new data over in case there is any
- reading the data and turning it into pandas GeoDataFrames
- ingesting the new data into the geoDB