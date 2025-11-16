{% macro impute_bedrooms_wc(Category, Number_Of_Bedrooms, Number_Of_WC, Living_Area) %}

    -- Bedrooms_Imputed
    CASE
        WHEN {{ Category }} = 'Land' THEN 0
        
        WHEN {{ Category }} IN ('Apartment', 'House') THEN
            CASE
                WHEN COALESCE({{ Number_Of_Bedrooms }}, 0) = 0
                 AND COALESCE({{ Number_Of_WC }},       0) = 0 THEN
                    CASE
                        WHEN {{ Living_Area }} IS NULL THEN 1
                        WHEN {{ Living_Area }} < 30 THEN 1
                        WHEN {{ Living_Area }} < 60 THEN 1
                        WHEN {{ Living_Area }} < 90 THEN 2
                        ELSE 2
                    END

                WHEN COALESCE({{ Number_Of_Bedrooms }}, 0) = 0 THEN
                    GREATEST(1, FLOOR({{ Living_Area }} / 40))

                WHEN COALESCE({{ Number_Of_WC }}, 0) = 0 THEN
                    {{ Number_Of_Bedrooms }}

                ELSE {{ Number_Of_Bedrooms }}
            END

        ELSE {{ Number_Of_Bedrooms }}
    END AS Bedrooms_Imputed,

    -- WC_Imputed
    CASE
        WHEN {{ Category }} = 'Land' THEN 0
        
        WHEN {{ Category }} IN ('Apartment', 'House') THEN
            CASE
                WHEN COALESCE({{ Number_Of_Bedrooms }}, 0) = 0
                 AND COALESCE({{ Number_Of_WC }},       0) = 0 THEN
                    CASE
                        WHEN {{ Living_Area }} IS NULL THEN 1
                        WHEN {{ Living_Area }} < 30 THEN 1
                        WHEN {{ Living_Area }} < 60 THEN 1
                        WHEN {{ Living_Area }} < 90 THEN 1
                        ELSE 2
                    END

                WHEN COALESCE({{ Number_Of_Bedrooms }}, 0) = 0 THEN
                    COALESCE(NULLIF({{ Number_Of_WC }}, 0), 1)

                WHEN COALESCE({{ Number_Of_WC }}, 0) = 0 THEN
                    GREATEST(1, FLOOR({{ Living_Area }} / 60))

                ELSE {{ Number_Of_WC }}
            END

        ELSE {{ Number_Of_WC }}
    END AS WC_Imputed

{% endmacro %}
