from datetime import datetime, timedelta

from faker import Faker


class SupportTicketGenerator:
    """Business logic for generating realistic synthetic support tickets."""

    def __init__(self, seed: int = 42):
        """Initialize the generator with a seed for reproducibility."""
        self.fake = Faker()
        Faker.seed(seed)

        # Business constants
        self.PRODUCT_LINES = ["Payment System", "Mobile App", "API Integration"]
        self.MESSAGE_TEMPLATES = self._get_message_templates()
        self.PRODUCT_SPECIFIC_DATA = self._get_product_specific_data()

    def _get_message_templates(self) -> list[str]:
        """Define realistic message templates for support tickets."""
        return [
            "I'm having trouble with my payment processing. The transaction keeps failing with error code {error_code}.",  # noqa: E501
            "The mobile app crashes when I try to {action}. This has been happening for {duration}.",  # noqa: E501
            "API integration is not working properly. Getting {http_status} errors when calling {endpoint}.",  # noqa: E501
            "My account balance is showing incorrectly. It shows {amount} but should be {correct_amount}.",  # noqa: E501
            "Unable to access {feature} feature. It shows '{error_message}' when I try to use it.",  # noqa: E501
            "The system is very slow today. It takes {time} to {action}.",  # noqa: E501
            "I need help with {task}. The documentation is unclear about {topic}.",  # noqa: E501
            "Getting timeout errors when trying to {action}. This is affecting my business operations.",  # noqa: E501
            "The new update broke {feature}. It was working fine before the update.",  # noqa: E501
            "I can't find the {feature} option in the interface. Where did it go?",  # noqa: E501
        ]

    def _get_product_specific_data(self) -> dict[str, dict[str, list[str]]]:
        """Define product-specific data for realistic message generation."""
        return {
            "Payment System": {
                "error_codes": ["PS001", "PS002", "PS003", "AUTH_FAILED"],
                "actions": ["process payment"],
                "durations": ["2 days", "1 week", "several hours"],
                "http_statuses": ["400", "401", "403", "500"],
                "endpoints": ["/api/payments"],
                "features": ["payment processing"],
                "error_messages": [
                    "Invalid card",
                    "Insufficient funds",
                    "Payment declined",
                ],
                "times": ["30 seconds", "2 minutes", "5 minutes"],
                "tasks": ["set up payment processing"],
                "topics": ["payment validation"],
            },
            "Mobile App": {
                "error_codes": ["MA001", "MA002", "CRASH_001"],
                "actions": ["login", "upload photo", "sync data"],
                "durations": ["3 days", "1 week", "since yesterday"],
                "http_statuses": ["404", "500", "502"],
                "endpoints": ["/api/mobile/sync"],
                "features": ["camera", "notifications", "sync"],
                "error_messages": [
                    "Network error",
                    "App not responding",
                    "Invalid session",
                ],
                "times": ["1 minute", "3 minutes", "forever"],
                "tasks": ["configure mobile app"],
                "topics": ["mobile app settings"],
            },
            "API Integration": {
                "error_codes": ["API001", "API002", "TIMEOUT"],
                "actions": ["make API call"],
                "durations": ["1 day", "several hours", "this morning"],
                "http_statuses": ["429", "500", "502", "503"],
                "endpoints": ["/api/v1/data", "/api/v1/users", "/api/v1/reports"],
                "features": ["API integration"],
                "error_messages": [
                    "Rate limit exceeded",
                    "Invalid API key",
                    "Service unavailable",
                ],
                "times": ["10 seconds", "30 seconds", "2 minutes"],
                "tasks": ["integrate with API"],
                "topics": ["API authentication"],
            },
        }

    def generate_message_text(self, product_line: str) -> str:
        """Generate a realistic message text for the given product line."""
        template = self.fake.random_element(self.MESSAGE_TEMPLATES)
        product_data = self.PRODUCT_SPECIFIC_DATA[product_line]

        # Generate amounts for balance-related messages
        amount = f"${self.fake.random_int(min=10, max=10000)}"
        correct_amount = f"${self.fake.random_int(min=10, max=10000)}"

        # For API Integration, use request-based amounts
        if product_line == "API Integration":
            amount = "1000 requests"
            correct_amount = "unlimited requests"
        elif product_line == "Mobile App":
            amount = self.fake.random_element(["100 points", "50 credits"])
            correct_amount = self.fake.random_element(["200 points", "100 credits"])

        # Format the template with product-specific data
        return template.format(
            error_code=self.fake.random_element(product_data["error_codes"]),
            action=self.fake.random_element(product_data["actions"]),
            duration=self.fake.random_element(product_data["durations"]),
            http_status=self.fake.random_element(product_data["http_statuses"]),
            endpoint=self.fake.random_element(product_data["endpoints"]),
            amount=amount,
            correct_amount=correct_amount,
            feature=self.fake.random_element(product_data["features"]),
            error_message=self.fake.random_element(product_data["error_messages"]),
            time=self.fake.random_element(product_data["times"]),
            task=self.fake.random_element(product_data["tasks"]),
            topic=self.fake.random_element(product_data["topics"]),
        )

    def generate_ticket_batch(
        self,
        start_id: int,
        batch_size: int,
        total_customers: int,
        days_back: int = 30,
    ) -> list[dict]:
        """Generate a batch of support tickets."""
        tickets = []

        for i in range(batch_size):
            ticket_id = start_id + i
            customer_id = self.fake.random_int(min=1, max=total_customers)
            product_line = self.fake.random_element(self.PRODUCT_LINES)
            message_text = self.generate_message_text(product_line)

            # Generate created_at within the specified time range
            created_at = self.fake.date_time_between(
                start_date=datetime.now() - timedelta(days=days_back),
                end_date=datetime.now(),
            )

            tickets.append(
                {
                    "ticket_id": ticket_id,
                    "customer_id": customer_id,
                    "product_line": product_line,
                    "message_text": message_text,
                    "created_at": created_at,
                }
            )

        return tickets


def generate_partition_tickets(
    partition_num: int,
    rows_per_partition: int,
    total_customers: int,
    seed: int = 42,
) -> list[dict]:
    """
    Generate tickets for a specific partition.

    Args:
        partition_num: The partition number (0-based)
        rows_per_partition: Number of tickets to generate
        total_customers: Total number of customers in the system
        seed: Random seed for reproducibility

    Returns:
        List of ticket dictionaries
    """
    generator = SupportTicketGenerator(seed=seed)
    start_id = partition_num * rows_per_partition + 1

    return generator.generate_ticket_batch(
        start_id=start_id,
        batch_size=rows_per_partition,
        total_customers=total_customers,
    )
