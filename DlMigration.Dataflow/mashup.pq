[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared vw_dim_customers = let
    Source = Sql.Databases("xt5ytmuaefdudosvbltiljpkaq-cmr3fg4zzi2ufphqc3hsobhbei.datawarehouse.fabric.microsoft.com"),
    Baraa_WH = Source{[Name="Baraa_WH"]}[Data],
    dbo_vw_dim_customers = Baraa_WH{[Schema="dbo",Item="vw_dim_customers"]}[Data]
in
    dbo_vw_dim_customers;
[BindToDefaultDestination = true]
shared Dates = let
    Source = Sql.Databases("xt5ytmuaefdudosvbltiljpkaq-cmr3fg4zzi2ufphqc3hsobhbei.datawarehouse.fabric.microsoft.com"),
    Baraa_WH = Source{[Name="Baraa_WH"]}[Data],
    dbo_Dates = Baraa_WH{[Schema="dbo",Item="Dates"]}[Data]
in
    dbo_Dates;
[BindToDefaultDestination = true]
shared vw_fact_sales = let
    Source = Sql.Databases("xt5ytmuaefdudosvbltiljpkaq-cmr3fg4zzi2ufphqc3hsobhbei.datawarehouse.fabric.microsoft.com"),
    Baraa_WH = Source{[Name="Baraa_WH"]}[Data],
    dbo_vw_fact_sales = Baraa_WH{[Schema="dbo",Item="vw_fact_sales"]}[Data]
in
    dbo_vw_fact_sales;
[BindToDefaultDestination = true]
shared vw_dim_products = let
    Source = Sql.Databases("xt5ytmuaefdudosvbltiljpkaq-cmr3fg4zzi2ufphqc3hsobhbei.datawarehouse.fabric.microsoft.com"),
    Baraa_WH = Source{[Name="Baraa_WH"]}[Data],
    dbo_vw_dim_products = Baraa_WH{[Schema="dbo",Item="vw_dim_products"]}[Data]
in
    dbo_vw_dim_products;
[BindToDefaultDestination = true]
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "9bb22313-ca99-4235-bcf0-16cf2704e122"]}[Data]{[lakehouseId = "2de54b2b-022b-474b-9bad-aba1e3befdbc"]}[Data];
