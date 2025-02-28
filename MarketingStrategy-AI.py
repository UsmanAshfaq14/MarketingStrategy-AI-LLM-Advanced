import json
import csv
import io
import re
import math

class MarketingStrategyAI:
    def __init__(self):
        self.required_fields = [
            "campaign_id", "engagement_score", "conversion_rate", "predictive_roi", 
            "budget_usd", "total_campaign_spend", "new_customers_acquired", 
            "customers_start", "customers_end", "revenue_generated", "advertising_spend"
        ]
        
        self.field_validations = {
            "engagement_score": lambda x: 0 <= x <= 100,
            "conversion_rate": lambda x: 0 <= x <= 1,
            "predictive_roi": lambda x: x > 0,
            "budget_usd": lambda x: x > 0,
            "total_campaign_spend": lambda x: x > 0,
            "new_customers_acquired": lambda x: x > 0 and isinstance(x, int),
            "customers_start": lambda x: x > 0 and isinstance(x, int),
            "customers_end": lambda x: x > 0 and isinstance(x, int),
            "revenue_generated": lambda x: x > 0,
            "advertising_spend": lambda x: x > 0
        }
        
        self.thresholds = {
            "composite_score": 50,
            "cac": 50,
            "retention_rate": 90,
            "roas": 3
        }

    def parse_data(self, data_str):
        """Parse input data from CSV or JSON format"""
        data_str = data_str.strip()
        
        if data_str.startswith('{'):
            try:
                json_data = json.loads(data_str)
                if 'campaigns' in json_data:
                    return json_data['campaigns']
                return [json_data]
            except json.JSONDecodeError:
                return {"error": "ERROR: Invalid data format. Please provide data in CSV or JSON format."}
        
        elif ',' in data_str:
            try:
                csv_reader = csv.DictReader(io.StringIO(data_str))
                return list(csv_reader)
            except Exception:
                return {"error": "ERROR: Invalid data format. Please provide data in CSV or JSON format."}
        
        else:
            return {"error": "ERROR: Invalid data format. Please provide data in CSV or JSON format."}

    def validate_data(self, data):
        """Validate the input data according to requirements"""
        validation_result = {
            "is_valid": True,
            "errors": [],
            "report": "",
            "data": []
        }
        
        # Check if we have any data
        if not data:
            validation_result["is_valid"] = False
            validation_result["errors"].append("ERROR: No data provided.")
            return validation_result
            
        # Check if data is already an error message
        if isinstance(data, dict) and 'error' in data:
            validation_result["is_valid"] = False
            validation_result["errors"].append(data['error'])
            return validation_result
            
        # Prepare validation report
        report = "# Data Validation Report\n"
        report += "## Data Structure Check:\n"
        report += f" - Number of campaigns: {len(data)}\n"
        
        # Count fields in first record
        if data:
            first_record = data[0]
            report += f" - Number of fields per record: {len(first_record)}\n\n"
        
        report += "## Required Fields Check:\n"
        
        # Create validated data collection
        validated_data = []
        
        # Check each record
        for idx, record in enumerate(data):
            # Convert string values to appropriate types
            converted_record = self._convert_field_types(record)
            
            # Check for required fields
            missing_fields = []
            for field in self.required_fields:
                if field not in converted_record:
                    missing_fields.append(field)
                    validation_result["is_valid"] = False
                    
            if missing_fields:
                error_msg = f"ERROR: Missing required field(s): {', '.join(missing_fields)} in row {idx+1}."
                validation_result["errors"].append(error_msg)
                continue
                
            # Validate field values
            invalid_fields = []
            for field, validation_func in self.field_validations.items():
                try:
                    if not validation_func(converted_record[field]):
                        invalid_fields.append(field)
                        validation_result["is_valid"] = False
                except (ValueError, TypeError):
                    invalid_fields.append(field)
                    validation_result["is_valid"] = False
                    
            if invalid_fields:
                error_msg = f"ERROR: Invalid value for the field(s): {', '.join(invalid_fields)} in row {idx+1}. Please correct and resubmit."
                validation_result["errors"].append(error_msg)
                continue
                
            # Add validated record to collection
            validated_data.append(converted_record)
        
        # Complete the validation report
        for field in self.required_fields:
            status = "present" if validation_result["is_valid"] else "missing"
            report += f" - {field}: {status}\n"
        
        report += "\n## Data Type and Value Validation:\n"
        
        for field, validation_func in self.field_validations.items():
            status = "validated" if validation_result["is_valid"] else "not valid"
            
            field_range = ""
            if field == "engagement_score":
                field_range = "(0 to 100)"
            elif field == "conversion_rate":
                field_range = "(0 to 1)"
            elif field in ["predictive_roi", "budget_usd", "total_campaign_spend", "revenue_generated", "advertising_spend"]:
                field_range = "(positive number)"
            elif field in ["new_customers_acquired", "customers_start", "customers_end"]:
                field_range = "(positive integer)"
                
            report += f" - {field} {field_range}: {status}\n"
            
        report += "\nValidation Summary:\n"
        if validation_result["is_valid"]:
            report += "Data validation is successful!"
        else:
            report += "\n".join(validation_result["errors"])
            
        validation_result["report"] = report
        validation_result["data"] = validated_data
        
        return validation_result

    def _convert_field_types(self, record):
        """Convert string values to appropriate types"""
        converted = {}
        for key, value in record.items():
            if key in ["new_customers_acquired", "customers_start", "customers_end"]:
                # Convert to integer
                try:
                    converted[key] = int(float(value))
                except (ValueError, TypeError):
                    converted[key] = value
            elif key in self.field_validations and key != "campaign_id":
                # Convert to float
                try:
                    converted[key] = float(value)
                except (ValueError, TypeError):
                    converted[key] = value
            else:
                converted[key] = value
        return converted

    def analyze_campaigns(self, validated_data):
        """Perform analysis on validated campaign data"""
        analysis_report = "# Campaign Analysis Summary:\n"
        analysis_report += f"Total Campaigns Evaluated: {len(validated_data)}\n\n"
        analysis_report += "# Detailed Analysis per Campaign:\n"
        
        for campaign in validated_data:
            campaign_id = campaign["campaign_id"]
            
            # Extract input data for report
            analysis_report += f"Campaign {campaign_id}\n"
            analysis_report += "Input Data:\n"
            analysis_report += f" - Engagement Score: {campaign['engagement_score']}\n"
            analysis_report += f" - Conversion Rate: {campaign['conversion_rate']}\n"
            analysis_report += f" - Predictive ROI: {campaign['predictive_roi']}\n"
            analysis_report += f" - Budget (USD): {campaign['budget_usd']}\n"
            analysis_report += f" - Total Campaign Spend: {campaign['total_campaign_spend']}\n"
            analysis_report += f" - New Customers Acquired: {campaign['new_customers_acquired']}\n"
            analysis_report += f" - Customers Start: {campaign['customers_start']}\n"
            analysis_report += f" - Customers End: {campaign['customers_end']}\n"
            analysis_report += f" - Revenue Generated: {campaign['revenue_generated']}\n"
            analysis_report += f" - Advertising Spend: {campaign['advertising_spend']}\n\n"
            
            # Calculate metrics
            cac = round(campaign["total_campaign_spend"] / campaign["new_customers_acquired"], 2)
            
            retention_rate = round(
                ((campaign["customers_end"] - campaign["new_customers_acquired"]) / campaign["customers_start"]) * 100, 2
            )
            
            roas = round(campaign["revenue_generated"] / campaign["advertising_spend"], 2)
            
            # Calculate composite score
            analysis_report += "# Detailed Calculations:\n"
            analysis_report += "## 1. Basic Metrics Calculations:\n"
            
            # CAC calculation
            analysis_report += "### a. Customer Acquisition Cost (CAC)\n"
            analysis_report += f" - Formula: $\\text{{CAC}} = \\frac{{\\text{{Total Campaign Spend}}}}{{\\text{{New Customers Acquired}}}}$\n"
            analysis_report += f" - Calculation: $\\text{{CAC}} = \\frac{{{campaign['total_campaign_spend']}}}{{{campaign['new_customers_acquired']}}} = {cac}$\n\n"
            
            # Retention Rate calculation
            analysis_report += "### b. Retention Rate\n"
            analysis_report += f" - Formula: $\\text{{Retention Rate}} = \\frac{{\\text{{customers_end}} - \\text{{New Customers Acquired}}}}{{\\text{{customers_start}}}} \\times 100$\n"
            analysis_report += f" - Calculation: $\\text{{Retention Rate}} = \\frac{{{campaign['customers_end']} - {campaign['new_customers_acquired']}}}{{{campaign['customers_start']}}} \\times 100 = {retention_rate}\\%$\n\n"
            
            # ROAS calculation
            analysis_report += "### c. Return on Advertising Spend (ROAS)\n"
            analysis_report += f" - Formula: $\\text{{ROAS}} = \\frac{{\\text{{Revenue Generated}}}}{{\\text{{Advertising Spend}}}}$\n"
            analysis_report += f" - Calculation: $\\text{{ROAS}} = \\frac{{{campaign['revenue_generated']}}}{{{campaign['advertising_spend']}}} = {roas}$\n\n"
            
            # Composite Score calculation
            analysis_report += "## 2. Composite Score Calculation:\n"
            analysis_report += " - Formula: $\\text{Composite Score} = (\\text{engagement_score} \\times 0.5) + ((\\text{conversion_rate} \\times 100) \\times 0.3) + (\\text{predictive_roi} \\times 10 \\times 0.2) - (\\text{CAC} \\times 0.05) + (\\text{Retention Rate} \\times 0.1) + (\\text{ROAS} \\times 0.1)$\n"
            
            analysis_report += "### Calculation Steps:\n"
            analysis_report += f"  Step 1: Convert conversion_rate to percentage: $\\text{{conversion_percentage}} = \\text{{conversion_rate}} \\times 100 = {campaign['conversion_rate']} \\times 100 = {campaign['conversion_rate'] * 100}$\n"
            
            # Calculate individual terms
            term1 = round(campaign["engagement_score"] * 0.5, 2)
            term2 = round((campaign["conversion_rate"] * 100) * 0.3, 2)
            term3 = round(campaign["predictive_roi"] * 10 * 0.2, 2)
            term4 = round(cac * 0.05, 2)
            term5 = round(retention_rate * 0.1, 2)
            term6 = round(roas * 0.1, 2)
            
            analysis_report += "  Step 2: Multiply each term by its weight:\n"
            analysis_report += f"   • Term1 = engagement_score $\\times 0.5 = {campaign['engagement_score']} \\times 0.5 = {term1}$\n"
            analysis_report += f"   • Term2 = conversion_percentage $\\times 0.3 = {campaign['conversion_rate'] * 100} \\times 0.3 = {term2}$\n"
            analysis_report += f"   • Term3 = predictive_roi $\\times 10 \\times 0.2 = {campaign['predictive_roi']} \\times 10 \\times 0.2 = {term3}$\n"
            analysis_report += f"   • Term4 = CAC $\\times 0.05 = {cac} \\times 0.05 = {term4}$ (to be subtracted)\n"
            analysis_report += f"   • Term5 = Retention Rate $\\times 0.1 = {retention_rate} \\times 0.1 = {term5}$\n"
            analysis_report += f"   • Term6 = ROAS $\\times 0.1 = {roas} \\times 0.1 = {term6}$\n"
            
            composite_score = round((term1 + term2 + term3) - term4 + term5 + term6, 2)
            
            analysis_report += f"  Step 3: Sum the weighted terms as: (Term1 + Term2 + Term3) - Term4 + Term5 + Term6 = ({term1} + {term2} + {term3}) - {term4} + {term5} + {term6} = {composite_score}\n"
            analysis_report += f" - Final Composite Score: {composite_score}\n\n"
            
            # Marketing strategy recommendation
            analysis_report += "## 3. Marketing Strategy Recommendation:\n"
            analysis_report += f" - IF Composite Score ≥ {self.thresholds['composite_score']}, THEN recommend maintaining or scaling up the campaign.\n"
            analysis_report += f" - ELSE IF Composite Score < {self.thresholds['composite_score']}, THEN recommend re-evaluating the campaign strategy to optimize engagement and budget allocation.\n\n"
            
            # Budget efficiency calculation
            cost_efficiency = round(composite_score / campaign["budget_usd"], 2)
            
            analysis_report += "## 4. Budget Efficiency Calculation:\n"
            analysis_report += " - Formula: $\\text{Cost Efficiency} = \\frac{\\text{Composite Score}}{\\text{budget_usd}}$\n"
            analysis_report += f" - Calculation: $\\text{{Cost Efficiency}} = \\frac{{{composite_score}}}{{{campaign['budget_usd']}}} = {cost_efficiency}$\n"
            
            if cost_efficiency > 0.05:
                budget_recommendation = "The campaign is considered cost-effective."
            else:
                budget_recommendation = "Recommend a reassessment of the campaign budget."
                
            analysis_report += f" - IF Cost Efficiency > 0.05, THEN the campaign is considered cost-effective; ELSE, recommend a reassessment of the campaign budget.\n"
            analysis_report += f" - Recommendation: {budget_recommendation}\n\n"
            
            # Final recommendation
            meets_thresholds = (
                composite_score >= self.thresholds["composite_score"] and
                cac <= self.thresholds["cac"] and
                retention_rate >= self.thresholds["retention_rate"] and
                roas >= self.thresholds["roas"]
            )
            
            analysis_report += "# Final Recommendation per Campaign:\n"
            analysis_report += f" - Composite Score: {composite_score}\n"
            analysis_report += f" - CAC: {cac} USD\n"
            analysis_report += f" - Retention Rate: {retention_rate}%\n"
            analysis_report += f" - ROAS: {roas}\n"
            
            if meets_thresholds:
                status = "Optimal"
                action = "Maintain or scale up the campaign."
            else:
                status = "Needs Improvement"
                action = "Re-evaluate the campaign strategy by increasing customer engagement, reducing campaign spend to lower CAC, implementing retention programs to improve the Retention Rate, and optimizing advertising spend to enhance ROAS."
                
            analysis_report += f" - Status: {status}\n"
            analysis_report += f" - Recommended Action: {action}\n\n"
            
            analysis_report += "---\n\n"
            
        analysis_report += "# FEEDBACK AND RATING PROTOCOL\n"
        analysis_report += "Would you like detailed calculations for any specific campaign? Please rate this analysis on a scale of 1-5.\n"
        
        return analysis_report

    def process_data(self, data_str):
        """Process data and return validation and analysis results"""
        # Parse data
        parsed_data = self.parse_data(data_str)
        
        # Validate data
        validation_result = self.validate_data(parsed_data)
        
        # Return validation report if validation failed
        if not validation_result["is_valid"]:
            return validation_result["report"]
            
        # Analyze data if validation successful
        analysis_report = self.analyze_campaigns(validation_result["data"])
        
        # Return complete report
        return validation_result["report"] + "\n\n" + analysis_report

def main():
    # Example usage
    print("Welcome to MarketingStrategy-AI!")
    print("Please input your campaign data in CSV or JSON format:")
    
    # In a real application, you would get input from the user
    # Here we'll use an example input
    example_input = """
    campaign_id,engagement_score,conversion_rate,predictive_roi,budget_usd,total_campaign_spend,new_customers_acquired,customers_start,customers_end,revenue_generated,advertising_spend
    CAMP601,80,0.25,7,1500,1400,30,300,330,2100,350
    CAMP602,75,0.20,6,1400,1300,28,280,310,2000,340
    CAMP603,85,0.27,8,1600,1500,32,320,350,2200,360
    CAMP604,90,0.22,9,1700,1600,35,350,380,2300,370
    CAMP605,70,0.15,5,1300,1200,25,250,270,1800,300
    CAMP606,95,0.30,10,1800,1700,38,380,420,2500,400
    CAMP607,65,0.18,6,1350,1250,27,270,300,1900,320

    """
    
    # Create the analyzer and process the data
    analyzer = MarketingStrategyAI()
    result = analyzer.process_data(example_input)
    
    # Print the result
    print(result)

if __name__ == "__main__":
    main()

