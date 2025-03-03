<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
    xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
    xmlns="http://www.opengis.net/sld"
    xmlns:ogc="http://www.opengis.net/ogc"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">

    <NamedLayer>
        <Name>ll_spain_landuse_dissolved</Name>
        <UserStyle>
            <Title>Simple Polygon Styling</Title>
            <FeatureTypeStyle>

                <Rule>
                    <Name>Urbano mixto</Name>
                    <Title>Urbano mixto</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Urbano mixto</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#641914</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#641914</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Industrial</Name>
                    <Title>Industrial</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Industrial</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#9d1f1b</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#9d1f1b</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Extracción minera</Name>
                    <Title>Extracción minera</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Extracción minera</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#6f4b4f</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#6f4b4f</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Infraestructuras de transporte</Name>
                    <Title>Infraestructuras de transporte</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Infraestructuras de transporte</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#321822</CssParameter>
                            <CssParameter name="fill-opacity">0.9</CssParameter>
                        </Fill>

                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Infraestructuras de transporte</Name>
                    <Title>Infraestructuras de transporte</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Infraestructuras de transporte</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#321822</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#321822</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Infraestructuras técnicas</Name>
                    <Title>Infraestructuras técnicas</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Infraestructuras técnicas</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#832814</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#832814</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Cultivos herbáceos</Name>
                    <Title>Cultivos herbáceos</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Cultivos herbáceos</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#dedba9</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#dedba9</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Invernaderos</Name>
                    <Title>Invernaderos</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Invernaderos</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#f8b964</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#f8b964</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Cultivos leñosos</Name>
                    <Title>Cultivos leñosos</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Cultivos leñosos</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#d3e4b8</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#d3e4b8</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Combinaciones de cultivos y vegetación</Name>
                    <Title>Combinaciones de cultivos y vegetación</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Combinaciones de cultivos y vegetación</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#beb581</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#beb581</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Pastizal</Name>
                    <Title>Pastizal</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Pastizal</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#a6b674</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#a6b674</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Matorral</Name>
                    <Title>Matorral</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Matorral</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#496227</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#496227</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>


                <Rule>
                    <Name>Bosque</Name>
                    <Title>Bosque</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Bosque</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#37461b</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#37461b</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Matorrales con arbolado</Name>
                    <Title>Matorrales con arbolado</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Matorrales con arbolado</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#959d6e</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#959d6e</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Pastizales con arbolado</Name>
                    <Title>Pastizales con arbolado</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Pastizales con arbolado</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#7a9b3c</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#7a9b3c</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Zonas sin vegetación</Name>
                    <Title>Zonas sin vegetación</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Zonas sin vegetación</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#a3a99e</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#a3a99e</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>

                <Rule>
                    <Name>Zonas húmedas</Name>
                    <Title>Zonas húmedas</Title>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>os_des_n2</ogc:PropertyName>
                            <ogc:Literal>Zonas húmedas</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <PolygonSymbolizer>
                        <Fill>
                            <CssParameter name="fill">#477590</CssParameter>
                            <CssParameter name="fill-opacity">1</CssParameter>
                        </Fill>
                        <Stroke>
                            <CssParameter name="stroke">#477590</CssParameter>
                            <CssParameter name="stroke-width">0.5</CssParameter>
                        </Stroke>
                    </PolygonSymbolizer>
                </Rule>


            </FeatureTypeStyle>
        </UserStyle>
    </NamedLayer>
</StyledLayerDescriptor>
